/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.portfolio.workingcapitalloan.service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import org.apache.fineract.infrastructure.core.service.MathUtil;
import org.apache.fineract.portfolio.loanproduct.domain.DueType;
import org.apache.fineract.portfolio.loanproduct.domain.PaymentAllocationTransactionType;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoan;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanBalance;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanCharge;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanPaymentAllocationRule;
import org.apache.fineract.portfolio.workingcapitalloanproduct.domain.WorkingCapitalPaymentAllocationType;
import org.springframework.stereotype.Component;

/**
 * Allocates a repayment-like amount across penalty charges, fee charges and principal following the payment allocation
 * order configured on the loan (copied from the product at creation). The same allocation is used by the forward
 * repayment flow and by {@link WorkingCapitalLoanTransactionReprocessingService} when transactions are replayed in a
 * changed chronological order.
 *
 * <p>
 * The processor mutates the passed {@link WorkingCapitalLoanCharge charges} (amount paid / paid flag) and
 * {@link WorkingCapitalLoanBalance balance} (principal/fee/penalty paid, overpayment) in place; persistence is the
 * caller's responsibility. When the loan has no configured allocation order the amount falls back to principal-only,
 * preserving the original behaviour for products that do not use charge allocation.
 */
@Component
public class WorkingCapitalLoanPaymentAllocationProcessor {

    /**
     * @param charges
     *            active charges of the loan, ordered oldest due date first (so the oldest outstanding charge is settled
     *            first within a bucket)
     */
    public AllocationResult allocate(final WorkingCapitalLoan loan, final WorkingCapitalLoanBalance balance,
            final List<WorkingCapitalLoanCharge> charges, final LocalDate transactionDate, final BigDecimal amount) {
        BigDecimal remaining = MathUtil.nullToZero(amount);
        BigDecimal principalPortion = BigDecimal.ZERO;
        BigDecimal feePortion = BigDecimal.ZERO;
        BigDecimal penaltyPortion = BigDecimal.ZERO;

        for (final WorkingCapitalPaymentAllocationType allocationType : resolveAllocationOrder(loan)) {
            if (!MathUtil.isGreaterThanZero(remaining)) {
                break;
            }
            final DueType dueType = allocationType.getDueType();
            switch (allocationType.getAllocationType()) {
                case PRINCIPAL -> {
                    final BigDecimal applied = allocateToPrincipal(balance, remaining);
                    principalPortion = principalPortion.add(applied);
                    remaining = remaining.subtract(applied);
                }
                case FEE -> {
                    final BigDecimal applied = allocateToCharges(charges, false, dueType, transactionDate, remaining, balance);
                    feePortion = feePortion.add(applied);
                    remaining = remaining.subtract(applied);
                }
                case PENALTY -> {
                    final BigDecimal applied = allocateToCharges(charges, true, dueType, transactionDate, remaining, balance);
                    penaltyPortion = penaltyPortion.add(applied);
                    remaining = remaining.subtract(applied);
                }
                default -> {
                }
            }
        }

        final BigDecimal overpayment = remaining.max(BigDecimal.ZERO);
        balance.setOverpaymentAmount(MathUtil.nullToZero(balance.getOverpaymentAmount()).add(overpayment));
        return new AllocationResult(principalPortion, feePortion, penaltyPortion, overpayment);
    }

    private BigDecimal allocateToPrincipal(final WorkingCapitalLoanBalance balance, final BigDecimal remaining) {
        final BigDecimal outstanding = MathUtil.nullToZero(balance.getPrincipalOutstanding());
        final BigDecimal applied = remaining.min(outstanding).max(BigDecimal.ZERO);
        balance.setPrincipalPaid(MathUtil.nullToZero(balance.getPrincipalPaid()).add(applied));
        return applied;
    }

    private BigDecimal allocateToCharges(final List<WorkingCapitalLoanCharge> charges, final boolean penalty, final DueType dueType,
            final LocalDate transactionDate, final BigDecimal remaining, final WorkingCapitalLoanBalance balance) {
        BigDecimal available = remaining;
        BigDecimal totalApplied = BigDecimal.ZERO;
        for (final WorkingCapitalLoanCharge charge : charges) {
            if (!MathUtil.isGreaterThanZero(available)) {
                break;
            }
            if (charge.isPenaltyCharge() != penalty || !matchesDueType(charge, dueType, transactionDate)) {
                continue;
            }
            final BigDecimal outstanding = charge.getAmountOutstanding();
            if (!MathUtil.isGreaterThanZero(outstanding)) {
                continue;
            }
            final BigDecimal applied = available.min(outstanding);
            charge.setAmountPaid(MathUtil.nullToZero(charge.getAmountPaid()).add(applied));
            if (!MathUtil.isGreaterThanZero(charge.getAmountOutstanding())) {
                charge.setPaid(true);
            }
            available = available.subtract(applied);
            totalApplied = totalApplied.add(applied);
        }
        if (MathUtil.isGreaterThanZero(totalApplied)) {
            if (penalty) {
                balance.setPenaltyPaid(MathUtil.nullToZero(balance.getPenaltyPaid()).add(totalApplied));
            } else {
                balance.setFeePaid(MathUtil.nullToZero(balance.getFeePaid()).add(totalApplied));
            }
        }
        return totalApplied;
    }

    private boolean matchesDueType(final WorkingCapitalLoanCharge charge, final DueType dueType, final LocalDate transactionDate) {
        final LocalDate dueDate = charge.getDueDate();
        // A charge with no due date is treated as already due; otherwise "due" means on/before the transaction date.
        final boolean isDue = dueDate == null || !dueDate.isAfter(transactionDate);
        return dueType == DueType.DUE ? isDue : !isDue;
    }

    private List<WorkingCapitalPaymentAllocationType> resolveAllocationOrder(final WorkingCapitalLoan loan) {
        final List<WorkingCapitalLoanPaymentAllocationRule> rules = loan.getPaymentAllocationRules();
        if (rules != null && !rules.isEmpty()) {
            final WorkingCapitalLoanPaymentAllocationRule repaymentRule = findRule(rules, PaymentAllocationTransactionType.REPAYMENT);
            final WorkingCapitalLoanPaymentAllocationRule rule = repaymentRule != null ? repaymentRule
                    : findRule(rules, PaymentAllocationTransactionType.DEFAULT);
            if (rule != null && rule.getAllocationTypes() != null && !rule.getAllocationTypes().isEmpty()) {
                return rule.getAllocationTypes();
            }
        }
        // No configured order: keep the legacy principal-only behaviour.
        return List.of(WorkingCapitalPaymentAllocationType.DUE_PRINCIPAL);
    }

    private WorkingCapitalLoanPaymentAllocationRule findRule(final List<WorkingCapitalLoanPaymentAllocationRule> rules,
            final PaymentAllocationTransactionType transactionType) {
        return rules.stream().filter(rule -> transactionType.equals(rule.getTransactionType())).findFirst().orElse(null);
    }

    public record AllocationResult(BigDecimal principalPortion, BigDecimal feeChargesPortion, BigDecimal penaltyChargesPortion,
            BigDecimal overpaymentPortion) {
    }
}
