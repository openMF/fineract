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
package org.apache.fineract.integrationtests.client.feign.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import org.apache.fineract.client.models.GetLoansLoanIdRepaymentPeriod;
import org.apache.fineract.client.models.GetLoansLoanIdResponse;
import org.apache.fineract.client.models.GetLoansLoanIdStatus;
import org.apache.fineract.client.models.PostLoanProductsRequest;
import org.apache.fineract.integrationtests.client.feign.FeignLoanTestBase;
import org.apache.fineract.integrationtests.client.feign.modules.LoanTestData;
import org.apache.fineract.integrationtests.common.Utils;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the sequence Reschedule -> ReAge -> Reschedule produces a correct repayment schedule.
 */
public class FeignLoanRescheduleAfterReAgeTest extends FeignLoanTestBase {

    @Test
    void testRescheduleAfterReAgeWithDownpayment() {
        runAt("2026-04-21", () -> {
            Long clientId = createClient("21 April 2026");
            Long productId = createLoanProduct(createProgressiveNoInterestWithDownpayment());

            Long loanId = createApproveAndDisburseProgressiveLoan(clientId, productId, "21 April 2026", 540.0, 2);

            GetLoansLoanIdResponse loan = getLoanDetails(loanId);
            verifyLoanStatus(loan, GetLoansLoanIdStatus::getActive);
            assertEquals(360.0, Utils.getDoubleValue(loan.getSummary().getTotalOutstanding()));

            createAndApproveReschedule(loanId, "21 April 2026", "21 May 2026", "21 June 2026");

            reAge(loanId, reAge("23 May 2026", LoanTestData.RepaymentFrequencyType.MONTHS_STRING, 1, 15));

            loan = getLoanDetails(loanId);
            assertEquals(360.0, Utils.getDoubleValue(loan.getSummary().getTotalOutstanding()));

            createAndApproveReschedule(loanId, "21 April 2026", "23 May 2026", "23 July 2026");

            loan = getLoanDetails(loanId);
            verifyLoanStatus(loan, GetLoansLoanIdStatus::getActive);

            double totalOutstanding = Utils.getDoubleValue(loan.getSummary().getTotalOutstanding());
            assertEquals(360.0, totalOutstanding, "Outstanding balance should equal principal minus downpayment");

            double totalPrincipalDue = loan.getRepaymentSchedule().getPeriods().stream()//
                    .filter(p -> p.getPeriod() != null)//
                    .mapToDouble(p -> Utils.getDoubleValue(p.getPrincipalDue()))//
                    .sum();
            assertEquals(540.0, totalPrincipalDue, "Total principal due must equal loan amount");

            List<GetLoansLoanIdRepaymentPeriod> periods = loan.getRepaymentSchedule().getPeriods();
            for (GetLoansLoanIdRepaymentPeriod period : periods) {
                if (period.getPrincipalLoanBalanceOutstanding() != null) {
                    assertTrue(Utils.getDoubleValue(period.getPrincipalLoanBalanceOutstanding()) >= 0,
                            "Balance should not be negative for period " + period.getPeriod());
                }
            }

            GetLoansLoanIdRepaymentPeriod firstUnpaid = periods.stream()//
                    .filter(p -> p.getPeriod() != null && Utils.getDoubleValue(p.getPrincipalOutstanding()) > 0)//
                    .findFirst().orElseThrow(() -> new AssertionError("Expected at least one unpaid installment"));
            assertEquals(2026, firstUnpaid.getDueDate().getYear());
            assertEquals(7, firstUnpaid.getDueDate().getMonthValue(), "First unpaid installment should be in July (shifted by 2 months)");
        });
    }

    @Test
    void testRescheduleAfterReAgeWithoutDownpayment() {
        runAt("2026-04-20", () -> {
            Long clientId = createClient("20 April 2026");
            Long productId = createLoanProduct(createProgressiveNoInterestNoDownpayment());

            Long loanId = createApproveAndDisburseProgressiveLoan(clientId, productId, "20 April 2026", 600.0, 1);

            GetLoansLoanIdResponse loan = getLoanDetails(loanId);
            verifyLoanStatus(loan, GetLoansLoanIdStatus::getActive);
            assertEquals(600.0, Utils.getDoubleValue(loan.getSummary().getTotalOutstanding()));

            createAndApproveReschedule(loanId, "20 April 2026", "20 May 2026", "18 August 2026");

            reAge(loanId, reAge("04 June 2026", LoanTestData.RepaymentFrequencyType.DAYS_STRING, 30, 20));

            loan = getLoanDetails(loanId);
            assertEquals(600.0, Utils.getDoubleValue(loan.getSummary().getTotalOutstanding()));

            createAndApproveReschedule(loanId, "20 April 2026", "04 July 2026", "02 October 2026");

            loan = getLoanDetails(loanId);
            verifyLoanStatus(loan, GetLoansLoanIdStatus::getActive);

            double totalOutstanding = Utils.getDoubleValue(loan.getSummary().getTotalOutstanding());
            assertEquals(600.0, totalOutstanding, "Outstanding balance should be 600");

            double totalPrincipalDue = loan.getRepaymentSchedule().getPeriods().stream()//
                    .filter(p -> p.getPeriod() != null)//
                    .mapToDouble(p -> Utils.getDoubleValue(p.getPrincipalDue()))//
                    .sum();
            assertEquals(600.0, totalPrincipalDue, "Total principal due must equal loan amount");

            List<GetLoansLoanIdRepaymentPeriod> periods = loan.getRepaymentSchedule().getPeriods();
            for (GetLoansLoanIdRepaymentPeriod period : periods) {
                if (period.getPrincipalLoanBalanceOutstanding() != null) {
                    assertTrue(Utils.getDoubleValue(period.getPrincipalLoanBalanceOutstanding()) >= 0,
                            "Balance should not be negative for period " + period.getPeriod());
                }
            }

            boolean hasOctoberInstallment = periods.stream()//
                    .filter(p -> p.getPeriod() != null)//
                    .anyMatch(p -> p.getDueDate().getMonthValue() == 10 && p.getDueDate().getYear() == 2026);
            assertTrue(hasOctoberInstallment, "Should have an installment shifted to October 2026");

            boolean hasJulyFourthInstallment = periods.stream()//
                    .filter(p -> p.getPeriod() != null)//
                    .anyMatch(p -> p.getDueDate().getMonthValue() == 7 && p.getDueDate().getDayOfMonth() == 4
                            && p.getDueDate().getYear() == 2026);
            assertFalse(hasJulyFourthInstallment, "Jul 4 installment should have been shifted");
        });
    }

    private PostLoanProductsRequest createProgressiveNoInterestWithDownpayment() {
        return customizeProduct(fourInstallmentsProgressiveWithAdvancedAllocation(), p -> p//
                .numberOfRepayments(2)//
                .interestRatePerPeriod(0.0)//
                .repaymentEvery(1)//
                .repaymentFrequencyType(LoanTestData.RepaymentFrequencyType.MONTHS_L)//
                .enableDownPayment(true)//
                .disbursedAmountPercentageForDownPayment(BigDecimal.valueOf(33.333333))//
                .enableAutoRepaymentForDownPayment(true)//
                .currencyCode("GBP"));
    }

    private PostLoanProductsRequest createProgressiveNoInterestNoDownpayment() {
        return customizeProduct(fourInstallmentsProgressiveWithAdvancedAllocation(), p -> p//
                .numberOfRepayments(1)//
                .interestRatePerPeriod(0.0)//
                .repaymentEvery(30)//
                .repaymentFrequencyType(LoanTestData.RepaymentFrequencyType.DAYS_L)//
                .enableDownPayment(false));
    }

}
