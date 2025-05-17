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
package org.apache.fineract.portfolio.loanaccount.service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.organisation.monetary.domain.MoneyHelper;
import org.apache.fineract.portfolio.charge.domain.ChargeCalculationType;
import org.apache.fineract.portfolio.loanaccount.data.ScheduleGeneratorDTO;
import org.apache.fineract.portfolio.loanaccount.domain.Loan;
import org.apache.fineract.portfolio.loanaccount.domain.LoanCharge;
import org.apache.fineract.portfolio.loanaccount.domain.LoanDisbursementDetails;
import org.apache.fineract.portfolio.loanaccount.domain.LoanTrancheDisbursementCharge;
import org.apache.fineract.portfolio.loanaccount.loanschedule.data.LoanScheduleDTO;
import org.apache.fineract.portfolio.loanaccount.loanschedule.domain.LoanScheduleModel;
import org.apache.fineract.portfolio.loanaccount.loanschedule.domain.LoanScheduleType;
import org.apache.fineract.portfolio.loanaccount.mapper.LoanMapper;
import org.apache.fineract.portfolio.loanaccount.service.schedule.LoanScheduleComponent;

@RequiredArgsConstructor
public class LoanScheduleService {

    private final LoanChargeService loanChargeService;
    private final ReprocessLoanTransactionsService reprocessLoanTransactionsService;
    private final LoanMapper loanMapper;
    private final LoanTransactionProcessingService loanTransactionProcessingService;
    private final LoanScheduleComponent loanSchedule;

    /**
     * Ability to regenerate the repayment schedule based on the loans current details/state.
     */
    public void regenerateRepaymentSchedule(final Loan loan, final ScheduleGeneratorDTO scheduleGeneratorDTO) {
        final LoanScheduleModel loanScheduleModel = loanMapper.regenerateScheduleModel(scheduleGeneratorDTO, loan);
        if (loanScheduleModel == null) {
            return;
        }
        loanSchedule.updateLoanSchedule(loan, loanScheduleModel);
        final Set<LoanCharge> charges = loan.getActiveCharges();
        for (final LoanCharge loanCharge : charges) {
            if (!loanCharge.isWaived()) {
                loanChargeService.recalculateLoanCharge(loan, loanCharge, scheduleGeneratorDTO.getPenaltyWaitPeriod());
            }
        }
    }

    public void recalculateSchedule(final Loan loan, final ScheduleGeneratorDTO generatorDTO) {
        if (loan.isInterestBearingAndInterestRecalculationEnabled() && !loan.isChargedOff()) {
            regenerateRepaymentScheduleWithInterestRecalculation(loan, generatorDTO);
        } else {
            regenerateRepaymentSchedule(loan, generatorDTO);
        }
        reprocessLoanTransactionsService.reprocessTransactions(loan);
    }

    public void recalculateScheduleFromLastTransaction(final Loan loan, final ScheduleGeneratorDTO generatorDTO,
            final List<Long> existingTransactionIds, final List<Long> existingReversedTransactionIds) {
        existingTransactionIds.addAll(loan.findExistingTransactionIds());
        existingReversedTransactionIds.addAll(loan.findExistingReversedTransactionIds());
        if (!loan.isProgressiveSchedule()) {
            if (loan.isInterestBearingAndInterestRecalculationEnabled() && !loan.isChargedOff()) {
                regenerateRepaymentScheduleWithInterestRecalculation(loan, generatorDTO);
            } else {
                regenerateRepaymentSchedule(loan, generatorDTO);
            }
            reprocessLoanTransactionsService.reprocessTransactions(loan);
        } else {
            reprocessLoanTransactionsService.updateModel(loan);
        }

    }

    public void regenerateRepaymentScheduleWithInterestRecalculation(final Loan loan, final ScheduleGeneratorDTO generatorDTO) {
        // Add null check for generatorDTO to prevent NullPointerException
        if (generatorDTO == null) {
            return;
        }
        
        final LocalDate lastTransactionDate = loan.getLastUserTransactionDate();
        final LoanScheduleDTO loanScheduleDTO = loanTransactionProcessingService.getRecalculatedSchedule(generatorDTO, loan);
        if (loanScheduleDTO == null) {
            return;
        }
        // Either the installments got recalculated or the model
        if (loanScheduleDTO.getInstallments() != null) {
            loanSchedule.updateLoanSchedule(loan, loanScheduleDTO.getInstallments());
        } else {
            loanSchedule.updateLoanSchedule(loan, loanScheduleDTO.getLoanScheduleModel());
        }
        loan.setInterestRecalculatedOn(DateUtils.getBusinessLocalDate());
        final LocalDate lastRepaymentDate = loan.getLastRepaymentPeriodDueDate(true);
        final Set<LoanCharge> charges = loan.getActiveCharges();
        for (final LoanCharge loanCharge : charges) {
            // Handle tranche disbursement charges
            if (loanCharge.isTrancheDisbursementCharge()) {
                // Get the related disbursement detail
                LoanTrancheDisbursementCharge trancheDisbursementCharge = loanCharge.getTrancheDisbursementCharge();
                if (trancheDisbursementCharge != null) {
                    LoanDisbursementDetails disbursementDetail = trancheDisbursementCharge.getloanDisbursementDetails();
                    if (disbursementDetail != null && disbursementDetail.actualDisbursementDate() != null) {
                        // Only process charges for disbursements that have actually happened
                        LocalDate actualDisbursementDate = disbursementDetail.actualDisbursementDate();
                        // Add null check to prevent NullPointerException
                        LocalDate recalculateFrom = generatorDTO.getRecalculateFrom();
                        if (recalculateFrom == null || actualDisbursementDate.equals(recalculateFrom) || actualDisbursementDate.isAfter(recalculateFrom)) {
                            // This is a relevant tranche disbursement - make sure the charge is calculated correctly
                            BigDecimal trancheAmount = disbursementDetail.principal();
                            BigDecimal calculatedAmount = calculateChargeAmountForTranche(
                                    loanCharge.getCharge().getChargeCalculation(), 
                                    loanCharge.getCharge().getAmount(), 
                                    trancheAmount);
                            
                            // Update the charge amount based on the actual tranche amount
                            loanCharge.update(calculatedAmount, null, null);
                            
                            // Ensure this charge is properly included in the schedule recalculation
                            if (loanCharge.isTrancheDisbursementChargeForDate(actualDisbursementDate)) {
                                // This charge is for the current disbursement date, ensure it's properly applied
                                loanCharge.setActive(true);
                            }
                        }
                    }
                }
            } else if (!loanCharge.isDueAtDisbursement()) {
                // Regular charge processing for non-disbursement charges
                loanChargeService.updateOverdueScheduleInstallment(loan, loanCharge);
                if (loanCharge.getDueLocalDate() == null || (!DateUtils.isBefore(lastRepaymentDate, loanCharge.getDueLocalDate())
                        || loan.getLoanProductRelatedDetail().getLoanScheduleType().equals(LoanScheduleType.PROGRESSIVE))) {
                    if ((loanCharge.isInstalmentFee() || !loanCharge.isWaived()) && (loanCharge.getDueLocalDate() == null
                            || !DateUtils.isAfter(lastTransactionDate, loanCharge.getDueLocalDate()))) {
                        loanChargeService.recalculateLoanCharge(loan, loanCharge, generatorDTO.getPenaltyWaitPeriod());
                        loanCharge.updateWaivedAmount(loan.getCurrency());
                    }
                } else {
                    loanCharge.setActive(false);
                }
            }
        }
        loanTransactionProcessingService.processPostDisbursementTransactions(loan);
    }

    public void handleRegenerateRepaymentScheduleWithInterestRecalculation(final Loan loan, final ScheduleGeneratorDTO generatorDTO) {
        regenerateRepaymentScheduleWithInterestRecalculation(loan, generatorDTO);
        reprocessLoanTransactionsService.reprocessTransactions(loan);
    }

    /**
     * Calculates the charge amount for a tranche disbursement based on the charge calculation type
     *
     * @param calculationType the charge calculation type
     * @param amountOrPercentageFromDefinition the amount or percentage from the charge definition
     * @param tranchePrincipal the principal amount for the tranche
     * @return the calculated charge amount
     */
    private BigDecimal calculateChargeAmountForTranche(final Integer calculationType, final BigDecimal amountOrPercentageFromDefinition,
            final BigDecimal tranchePrincipal) {
        BigDecimal calculatedAmount = BigDecimal.ZERO;
        switch (ChargeCalculationType.fromInt(calculationType)) {
            case FLAT:
                calculatedAmount = amountOrPercentageFromDefinition;
                break;
            case PERCENT_OF_AMOUNT:
            case PERCENT_OF_AMOUNT_AND_INTEREST:
            case PERCENT_OF_INTEREST:
            case PERCENT_OF_DISBURSEMENT_AMOUNT:
                // For tranche disbursement charges with percentage calculation,
                // we need to ensure we're calculating the correct percentage
                // For example, if the charge is 1% and the tranche amount is 30.0,
                // the charge should be 0.3 (30.0 * 0.01)
                BigDecimal percentageDecimal = amountOrPercentageFromDefinition.divide(BigDecimal.valueOf(100), 8, BigDecimal.ROUND_HALF_EVEN);
                calculatedAmount = tranchePrincipal.multiply(percentageDecimal, MoneyHelper.getMathContext());
                break;
            default:
                calculatedAmount = amountOrPercentageFromDefinition;
                break;
        }
        // Ensure consistent scale and rounding for all charge amounts
        return calculatedAmount.setScale(2, MoneyHelper.getMathContext().getRoundingMode());
    }

    /**
     * Calculates a percentage of an amount
     *
     * @param amount the base amount
     * @param percentage the percentage to calculate
     * @return the calculated amount
     */
    private static BigDecimal percentageOf(final BigDecimal amount, final BigDecimal percentage) {
        // For tranche disbursement charges, we need to ensure we're calculating correctly
        // The percentage value from the charge definition is the actual percentage (e.g., 1.0 for 1%)
        // We need to divide by 100 to get the decimal multiplier
        return amount.multiply(percentage, MoneyHelper.getMathContext())
                .divide(BigDecimal.valueOf(100L), MoneyHelper.getMathContext())
                .setScale(6, MoneyHelper.getMathContext().getRoundingMode());
    }
}
