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
package org.apache.fineract.integrationtests.client.feign.modules;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.fineract.client.models.AdvancedPaymentData;
import org.apache.fineract.client.models.PaymentAllocationOrder;
import org.apache.fineract.client.models.PostCreateRescheduleLoansRequest;
import org.apache.fineract.client.models.PostLoansLoanIdRequest;
import org.apache.fineract.client.models.PostLoansLoanIdTransactionsRequest;
import org.apache.fineract.client.models.PostLoansRequest;
import org.apache.fineract.client.models.PostUpdateRescheduleLoansRequest;

public final class LoanRequestBuilders {

    private LoanRequestBuilders() {}

    public static PostLoansRequest applyLoan(Long clientId, Long productId, String submittedOnDate, Double principal,
            Integer numberOfRepayments) {
        return new PostLoansRequest()//
                .clientId(clientId)//
                .productId(productId)//
                .loanType("individual")//
                .submittedOnDate(submittedOnDate)//
                .expectedDisbursementDate(submittedOnDate)//
                .principal(BigDecimal.valueOf(principal))//
                .loanTermFrequency(numberOfRepayments)//
                .loanTermFrequencyType(LoanTestData.RepaymentFrequencyType.MONTHS)//
                .numberOfRepayments(numberOfRepayments)//
                .repaymentEvery(1)//
                .repaymentFrequencyType(LoanTestData.RepaymentFrequencyType.MONTHS)//
                .interestRatePerPeriod(BigDecimal.ZERO)//
                .amortizationType(LoanTestData.AmortizationType.EQUAL_INSTALLMENTS)//
                .interestType(LoanTestData.InterestType.DECLINING_BALANCE)//
                .interestCalculationPeriodType(LoanTestData.InterestCalculationPeriodType.SAME_AS_REPAYMENT_PERIOD)//
                .transactionProcessingStrategyCode("mifos-standard-strategy")//
                .locale(LoanTestData.LOCALE)//
                .dateFormat(LoanTestData.DATETIME_PATTERN);
    }

    public static PostLoansRequest applyCumulativeLoan(Long clientId, Long productId, String submittedOnDate, Double principal,
            Integer numberOfRepayments, Double interestRate) {
        return new PostLoansRequest()//
                .clientId(clientId)//
                .productId(productId)//
                .submittedOnDate(submittedOnDate)//
                .expectedDisbursementDate(submittedOnDate)//
                .principal(BigDecimal.valueOf(principal))//
                .loanTermFrequency(numberOfRepayments)//
                .loanTermFrequencyType(LoanTestData.RepaymentFrequencyType.MONTHS)//
                .numberOfRepayments(numberOfRepayments)//
                .repaymentEvery(1)//
                .repaymentFrequencyType(LoanTestData.RepaymentFrequencyType.MONTHS)//
                .interestRatePerPeriod(BigDecimal.valueOf(interestRate))//
                .amortizationType(LoanTestData.AmortizationType.EQUAL_INSTALLMENTS)//
                .interestType(LoanTestData.InterestType.DECLINING_BALANCE)//
                .interestCalculationPeriodType(LoanTestData.InterestCalculationPeriodType.DAILY)//
                .transactionProcessingStrategyCode("due-penalty-fee-interest-principal-in-advance-principal-penalty-fee-interest-strategy")//
                .loanType("individual")//
                .locale(LoanTestData.LOCALE)//
                .dateFormat(LoanTestData.DATETIME_PATTERN);
    }

    public static PostLoansRequest applyProgressiveLoan(Long clientId, Long productId, String submittedOnDate, Double principal,
            Integer numberOfRepayments, Double interestRate) {
        return applyCumulativeLoan(clientId, productId, submittedOnDate, principal, numberOfRepayments, interestRate)
                .transactionProcessingStrategyCode(LoanTestData.TransactionProcessingStrategyCode.ADVANCED_PAYMENT_ALLOCATION_STRATEGY);
    }

    public static PostLoansRequest applyProgressiveLoan(Long clientId, Long productId, String submittedOnDate, Double principal,
            Integer numberOfRepayments) {
        return applyLoan(clientId, productId, submittedOnDate, principal, numberOfRepayments)
                .transactionProcessingStrategyCode(LoanTestData.TransactionProcessingStrategyCode.ADVANCED_PAYMENT_ALLOCATION_STRATEGY);
    }

    public static PostLoansLoanIdRequest approveLoan(Double approvedAmount, String approvedOnDate) {
        return new PostLoansLoanIdRequest()//
                .approvedLoanAmount(BigDecimal.valueOf(approvedAmount))//
                .approvedOnDate(approvedOnDate)//
                .locale(LoanTestData.LOCALE)//
                .dateFormat(LoanTestData.DATETIME_PATTERN);
    }

    public static PostLoansLoanIdRequest disburseLoan(Double disbursedAmount, String disbursedOnDate) {
        return new PostLoansLoanIdRequest()//
                .actualDisbursementDate(disbursedOnDate)//
                .transactionAmount(BigDecimal.valueOf(disbursedAmount))//
                .locale(LoanTestData.LOCALE)//
                .dateFormat(LoanTestData.DATETIME_PATTERN);
    }

    public static PostLoansLoanIdTransactionsRequest repayLoan(Double amount, String transactionDate) {
        PostLoansLoanIdTransactionsRequest request = new PostLoansLoanIdTransactionsRequest();
        request.setTransactionDate(transactionDate);
        request.setTransactionAmount(amount);
        request.setLocale(LoanTestData.LOCALE);
        request.setDateFormat(LoanTestData.DATETIME_PATTERN);
        return request;
    }

    public static PostLoansLoanIdTransactionsRequest makeWaiver(Double amount, String transactionDate) {
        PostLoansLoanIdTransactionsRequest request = new PostLoansLoanIdTransactionsRequest();
        request.setTransactionDate(transactionDate);
        request.setTransactionAmount(amount);
        request.setLocale(LoanTestData.LOCALE);
        request.setDateFormat(LoanTestData.DATETIME_PATTERN);
        return request;
    }

    public static PostLoansLoanIdTransactionsRequest chargeOff(String transactionDate) {
        PostLoansLoanIdTransactionsRequest request = new PostLoansLoanIdTransactionsRequest();
        request.setTransactionDate(transactionDate);
        request.setLocale(LoanTestData.LOCALE);
        request.setDateFormat(LoanTestData.DATETIME_PATTERN);
        return request;
    }

    public static PostLoansLoanIdTransactionsRequest addChargeback(Long transactionId, Double amount, String transactionDate) {
        PostLoansLoanIdTransactionsRequest request = new PostLoansLoanIdTransactionsRequest();
        request.setTransactionDate(transactionDate);
        request.setTransactionAmount(amount);
        request.setLocale(LoanTestData.LOCALE);
        request.setDateFormat(LoanTestData.DATETIME_PATTERN);
        return request;
    }

    public static PostLoansLoanIdTransactionsRequest waiveInterest(Double amount, String transactionDate) {
        return makeWaiver(amount, transactionDate);
    }

    /**
     * Creates a reschedule request that shifts a due date. Uses rescheduleReasonId=1 (default seed data).
     */
    public static PostCreateRescheduleLoansRequest rescheduleRequest(Long loanId, String submittedOnDate, String rescheduleFromDate,
            String adjustedDueDate) {
        return rescheduleRequest(loanId, submittedOnDate, rescheduleFromDate, adjustedDueDate, 1L);
    }

    public static PostCreateRescheduleLoansRequest rescheduleRequest(Long loanId, String submittedOnDate, String rescheduleFromDate,
            String adjustedDueDate, Long rescheduleReasonId) {
        return new PostCreateRescheduleLoansRequest()//
                .loanId(loanId)//
                .submittedOnDate(submittedOnDate)//
                .rescheduleFromDate(rescheduleFromDate)//
                .adjustedDueDate(adjustedDueDate)//
                .rescheduleReasonId(rescheduleReasonId)//
                .locale(LoanTestData.LOCALE)//
                .dateFormat(LoanTestData.DATETIME_PATTERN);
    }

    public static PostUpdateRescheduleLoansRequest approveReschedule(String approvedOnDate) {
        return new PostUpdateRescheduleLoansRequest()//
                .approvedOnDate(approvedOnDate)//
                .locale(LoanTestData.LOCALE)//
                .dateFormat(LoanTestData.DATETIME_PATTERN);
    }

    /**
     * Creates a reAge request for non-interest-bearing loans (no interest handling needed).
     */
    public static PostLoansLoanIdTransactionsRequest reAge(String startDate, String frequencyType, Integer frequencyNumber,
            Integer numberOfInstallments) {
        return reAge(startDate, frequencyType, frequencyNumber, numberOfInstallments, null);
    }

    /**
     * Creates a reAge request with explicit interest handling.
     *
     * @param reAgeInterestHandling
     *            e.g. "EQUAL_AMORTIZATION_PAYABLE_INTEREST", "EQUAL_AMORTIZATION_FULL_INTEREST", or null for
     *            non-interest-bearing loans
     */
    public static PostLoansLoanIdTransactionsRequest reAge(String startDate, String frequencyType, Integer frequencyNumber,
            Integer numberOfInstallments, String reAgeInterestHandling) {
        PostLoansLoanIdTransactionsRequest request = new PostLoansLoanIdTransactionsRequest();
        request.setStartDate(startDate);
        request.setFrequencyType(frequencyType);
        request.setFrequencyNumber(frequencyNumber);
        request.setNumberOfInstallments(numberOfInstallments);
        if (reAgeInterestHandling != null) {
            request.setReAgeInterestHandling(reAgeInterestHandling);
        }
        request.setLocale(LoanTestData.LOCALE);
        request.setDateFormat(LoanTestData.DATETIME_PATTERN);
        return request;
    }

    /**
     * Creates a DEFAULT payment allocation with NEXT_INSTALLMENT future rule. Suitable for most progressive loan
     * products using advanced-payment-allocation-strategy.
     */
    public static AdvancedPaymentData defaultPaymentAllocation() {
        return paymentAllocation("DEFAULT", "NEXT_INSTALLMENT");
    }

    /**
     * Creates a payment allocation for a specific transaction type and future installment allocation rule.
     *
     * @param transactionType
     *            e.g. "DEFAULT", "REPAYMENT", "DOWN_PAYMENT", "MERCHANT_ISSUED_REFUND"
     * @param futureInstallmentAllocationRule
     *            e.g. "NEXT_INSTALLMENT", "LAST_INSTALLMENT", "NEXT_LAST_INSTALLMENT"
     */
    public static AdvancedPaymentData paymentAllocation(String transactionType, String futureInstallmentAllocationRule) {
        AdvancedPaymentData data = new AdvancedPaymentData();
        data.setTransactionType(transactionType);
        data.setFutureInstallmentAllocationRule(futureInstallmentAllocationRule);
        AtomicInteger order = new AtomicInteger(1);
        List<PaymentAllocationOrder> orders = Stream
                .of("PAST_DUE_PENALTY", "PAST_DUE_FEE", "PAST_DUE_PRINCIPAL", "PAST_DUE_INTEREST", "DUE_PENALTY", "DUE_FEE",
                        "DUE_PRINCIPAL", "DUE_INTEREST", "IN_ADVANCE_PENALTY", "IN_ADVANCE_FEE", "IN_ADVANCE_PRINCIPAL",
                        "IN_ADVANCE_INTEREST")
                .map(rule -> new PaymentAllocationOrder().paymentAllocationRule(rule).order(order.getAndIncrement())).toList();
        data.setPaymentAllocationOrder(orders);
        return data;
    }
}
