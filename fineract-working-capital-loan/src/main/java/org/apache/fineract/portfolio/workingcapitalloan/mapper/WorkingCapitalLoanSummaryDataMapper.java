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
package org.apache.fineract.portfolio.workingcapitalloan.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.fineract.infrastructure.core.config.MapstructMapperConfig;
import org.apache.fineract.infrastructure.core.service.MathUtil;
import org.apache.fineract.organisation.monetary.data.CurrencyData;
import org.apache.fineract.portfolio.loanaccount.domain.LoanTransactionType;
import org.apache.fineract.portfolio.workingcapitalloan.data.WorkingCapitalLoanSummaryData;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoan;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanTransaction;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanTransactionAllocation;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;

@Mapper(config = MapstructMapperConfig.class)
public interface WorkingCapitalLoanSummaryDataMapper {

    @Named("toSummaryData")
    @Mapping(target = "currency", source = ".", qualifiedByName = "toCurrency")
    // Principal
    @Mapping(target = "principalDisbursed", source = "transactions", qualifiedByName = "toPrincipalDisbursed")
    @Mapping(target = "principalPaid", source = "balance.totalPaidPrincipal", qualifiedByName = "nullToZero")
    @Mapping(target = "principalOutstanding", source = "balance.principalOutstanding", qualifiedByName = "nullToZero")
    // Discount fee
    @Mapping(target = "discountCharged", source = "transactions", qualifiedByName = "toDiscountCharged")
    @Mapping(target = "discountPaid", source = "transactions", qualifiedByName = "toDiscountPaid")
    @Mapping(target = "discountOutstanding", source = "transactions", qualifiedByName = "toDiscountOutstanding")
    // Income recognition
    @Mapping(target = "realizedIncome", source = "balance.realizedIncome", qualifiedByName = "nullToZero")
    @Mapping(target = "unrealizedIncome", source = "balance.unrealizedIncome", qualifiedByName = "nullToZero")
    // Overpayment
    @Mapping(target = "overpaymentAmount", source = "balance.overpaymentAmount", qualifiedByName = "nullToZero")
    // Aggregates
    @Mapping(target = "totalExpectedRepayment", source = "transactions", qualifiedByName = "toTotalExpectedRepayment")
    @Mapping(target = "totalRepayment", source = "balance.totalPayment", qualifiedByName = "nullToZero")
    @Mapping(target = "totalOutstanding", source = ".", qualifiedByName = "toTotalOutstanding")
    // Transaction summaries
    @Mapping(target = "totalDisbursement", source = "transactions", qualifiedByName = "toPrincipalDisbursed")
    @Mapping(target = "totalRepaymentTransaction", source = "transactions", qualifiedByName = "toTotalRepaymentTransaction")
    @Mapping(target = "totalDiscountFee", source = "transactions", qualifiedByName = "toDiscountCharged")
    WorkingCapitalLoanSummaryData toData(WorkingCapitalLoan loan);

    @Named("toCurrency")
    default CurrencyData toCurrency(final WorkingCapitalLoan loan) {
        return loan.getLoanProduct().getCurrency().toData();
    }

    @Named("nullToZero")
    default BigDecimal nullToZero(final BigDecimal value) {
        return MathUtil.nullToZero(value);
    }

    @Named("toPrincipalDisbursed")
    default BigDecimal toPrincipalDisbursed(final List<WorkingCapitalLoanTransaction> transactions) {
        return sumActive(transactions, LoanTransactionType.DISBURSEMENT);
    }

    @Named("toDiscountCharged")
    default BigDecimal toDiscountCharged(final List<WorkingCapitalLoanTransaction> transactions) {
        return sumActive(transactions, LoanTransactionType.DISCOUNT_FEE);
    }

    @Named("toDiscountPaid")
    default BigDecimal toDiscountPaid(final List<WorkingCapitalLoanTransaction> transactions) {
        return sumActiveAllocationField(transactions, WorkingCapitalLoanTransactionAllocation::getFeeChargesPortion);
    }

    @Named("toDiscountOutstanding")
    default BigDecimal toDiscountOutstanding(final List<WorkingCapitalLoanTransaction> transactions) {
        return toDiscountCharged(transactions).subtract(toDiscountPaid(transactions));
    }

    @Named("toTotalExpectedRepayment")
    default BigDecimal toTotalExpectedRepayment(final List<WorkingCapitalLoanTransaction> transactions) {
        return toPrincipalDisbursed(transactions).add(toDiscountCharged(transactions));
    }

    @Named("toTotalOutstanding")
    default BigDecimal toTotalOutstanding(final WorkingCapitalLoan loan) {
        return nullToZero(loan.getBalance() != null ? loan.getBalance().getPrincipalOutstanding() : null)
                .add(toDiscountOutstanding(loan.getTransactions()));
    }

    @Named("toTotalRepaymentTransaction")
    default BigDecimal toTotalRepaymentTransaction(final List<WorkingCapitalLoanTransaction> transactions) {
        return sumActive(transactions, LoanTransactionType.REPAYMENT);
    }

    private BigDecimal sumActive(final List<WorkingCapitalLoanTransaction> transactions, final LoanTransactionType type) {
        return transactions.stream().filter(t -> t.getTypeOf() == type && !t.isReversed())
                .map(WorkingCapitalLoanTransaction::getTransactionAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal sumActiveAllocationField(final List<WorkingCapitalLoanTransaction> transactions,
            final Function<WorkingCapitalLoanTransactionAllocation, BigDecimal> extractor) {
        return transactions.stream().filter(t -> !t.isReversed() && t.getAllocation() != null).map(t -> extractor.apply(t.getAllocation()))
                .filter(Objects::nonNull).reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
