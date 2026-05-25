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
import org.apache.fineract.client.models.PostWorkingCapitalLoanTransactionsRequest;
import org.apache.fineract.client.models.PostWorkingCapitalLoansLoanIdRequest;
import org.apache.fineract.client.models.PostWorkingCapitalLoansRequest;
import org.apache.fineract.client.models.PutWorkingCapitalLoansLoanIdRateRequest;

public final class WorkingCapitalLoanRequestBuilders {

    private static final String LOCALE = "en";
    private static final String DATE_FORMAT = "dd MMMM yyyy";

    private WorkingCapitalLoanRequestBuilders() {}

    public static PostWorkingCapitalLoansRequest submitApplication(Long clientId, Long productId, BigDecimal principal,
            BigDecimal periodPaymentRate, String submittedOnDate, String expectedDisbursementDate) {
        return new PostWorkingCapitalLoansRequest().clientId(clientId).productId(productId).principalAmount(principal)
                .periodPaymentRate(periodPaymentRate).submittedOnDate(submittedOnDate).expectedDisbursementDate(expectedDisbursementDate)
                .totalPayment(BigDecimal.valueOf(100000)).locale(LOCALE).dateFormat(DATE_FORMAT);
    }

    public static PostWorkingCapitalLoansLoanIdRequest approve(String approvedOnDate, BigDecimal approvedAmount,
            String expectedDisbursementDate) {
        return new PostWorkingCapitalLoansLoanIdRequest().approvedOnDate(approvedOnDate).approvedLoanAmount(approvedAmount)
                .expectedDisbursementDate(expectedDisbursementDate).locale(LOCALE).dateFormat(DATE_FORMAT);
    }

    public static PostWorkingCapitalLoansLoanIdRequest approveWithDiscount(String approvedOnDate, BigDecimal approvedAmount,
            String expectedDisbursementDate, BigDecimal discountAmount) {
        return new PostWorkingCapitalLoansLoanIdRequest().approvedOnDate(approvedOnDate).approvedLoanAmount(approvedAmount)
                .expectedDisbursementDate(expectedDisbursementDate).discountAmount(discountAmount).locale(LOCALE).dateFormat(DATE_FORMAT);
    }

    public static PostWorkingCapitalLoansLoanIdRequest disburse(String actualDisbursementDate, BigDecimal transactionAmount) {
        return new PostWorkingCapitalLoansLoanIdRequest().actualDisbursementDate(actualDisbursementDate)
                .transactionAmount(transactionAmount).locale(LOCALE).dateFormat(DATE_FORMAT);
    }

    public static PostWorkingCapitalLoansLoanIdRequest disburseWithDiscount(String actualDisbursementDate, BigDecimal transactionAmount,
            BigDecimal discountAmount) {
        return new PostWorkingCapitalLoansLoanIdRequest().actualDisbursementDate(actualDisbursementDate)
                .transactionAmount(transactionAmount).discountAmount(discountAmount).locale(LOCALE).dateFormat(DATE_FORMAT);
    }

    public static PostWorkingCapitalLoansLoanIdRequest undoDisbursal() {
        return new PostWorkingCapitalLoansLoanIdRequest().locale(LOCALE).dateFormat(DATE_FORMAT);
    }

    public static PostWorkingCapitalLoansLoanIdRequest emptyCommand() {
        return new PostWorkingCapitalLoansLoanIdRequest().locale(LOCALE).dateFormat(DATE_FORMAT);
    }

    public static PutWorkingCapitalLoansLoanIdRateRequest updateRate(BigDecimal newRate) {
        return new PutWorkingCapitalLoansLoanIdRateRequest().periodPaymentRate(newRate).locale(LOCALE);
    }

    public static PostWorkingCapitalLoanTransactionsRequest repayment(BigDecimal amount, String transactionDate) {
        return new PostWorkingCapitalLoanTransactionsRequest().transactionAmount(amount).transactionDate(transactionDate).locale(LOCALE)
                .dateFormat(DATE_FORMAT);
    }
}
