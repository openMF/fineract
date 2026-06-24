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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.apache.fineract.client.models.GetWorkingCapitalLoanTransactionIdResponse;
import org.apache.fineract.client.models.GetWorkingCapitalLoansLoanIdResponse;
import org.apache.fineract.integrationtests.client.FeignIntegrationTest;
import org.apache.fineract.integrationtests.client.feign.helpers.FeignBusinessDateHelper;
import org.apache.fineract.integrationtests.client.feign.helpers.FeignClientHelper;
import org.apache.fineract.integrationtests.client.feign.helpers.FeignWorkingCapitalLoanHelper;
import org.apache.fineract.integrationtests.client.feign.modules.WorkingCapitalLoanRequestBuilders;
import org.apache.fineract.integrationtests.common.Utils;
import org.apache.fineract.integrationtests.common.workingcapitalloanproduct.WorkingCapitalLoanProductHelper;
import org.apache.fineract.integrationtests.common.workingcapitalloanproduct.WorkingCapitalLoanProductTestBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for WC Transaction Reprocessing (generic).
 *
 * Backdated repayments are applied through the regular incremental flow (balance math is order-independent and the
 * amortization model records payments on their actual day). The reprocessing engine only recalculates allocations of
 * subsequent transactions when payments compete for charge buckets — without charges it is a no-op, which these tests
 * verify by asserting that existing allocations stay untouched after a backdated repayment.
 *
 * <p>
 * The charge-based re-allocation path (fees/penalties competing across transactions) is covered at the E2E layer in
 * {@code WorkingCapitalTransactionReprocessing.feature} (C85212/C85216/C85218); this integration suite focuses on the
 * charge-free, order-independent path.
 */
public class FeignWorkingCapitalLoanTransactionReprocessingTest extends FeignIntegrationTest {

    private FeignWorkingCapitalLoanHelper wcLoanHelper;
    private FeignClientHelper clientHelper;
    private FeignBusinessDateHelper businessDateHelper;
    private WorkingCapitalLoanProductHelper productHelper;

    private final List<Long> createdLoanIds = new ArrayList<>();
    private final List<Long> createdProductIds = new ArrayList<>();

    @BeforeAll
    void setupHelpers() {
        wcLoanHelper = new FeignWorkingCapitalLoanHelper(fineractClient());
        clientHelper = new FeignClientHelper(fineractClient());
        businessDateHelper = new FeignBusinessDateHelper(fineractClient());
        productHelper = new WorkingCapitalLoanProductHelper();
    }

    @AfterAll
    void cleanupEntities() {
        createdLoanIds.forEach(wcLoanHelper::cleanupLoan);
        createdLoanIds.clear();
        createdProductIds.clear();
    }

    @Test
    void testBackdatedRepayment_balanceReflectsBothPayments() {
        businessDateHelper.runAt("2026-01-01", () -> {
            Long clientForTest = clientHelper.createClient("01 January 2026");
            Long loanId = createAndDisburseLoanOnDate(clientForTest, BigDecimal.valueOf(9000), "01 January 2026");

            // First repayment on day 10
            businessDateHelper.updateBusinessDate("BUSINESS_DATE", "2026-01-10");
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(3000), "10 January 2026"));

            GetWorkingCapitalLoansLoanIdResponse afterFirstRepayment = wcLoanHelper.getLoanDetails(loanId);
            assertNotNull(afterFirstRepayment.getBalance(), "Balance should exist after repayment");
            assertEqualBigDecimal(BigDecimal.valueOf(6000), afterFirstRepayment.getBalance().getPrincipalOutstanding(),
                    "Outstanding should be 6000 after 3000 repayment on 9000 loan");
            assertEqualBigDecimal(BigDecimal.valueOf(3000), afterFirstRepayment.getBalance().getPrincipalPaid(),
                    "Principal paid should be 3000 after first repayment");

            // Backdated repayment on day 5 (before existing repayment on day 10)
            businessDateHelper.updateBusinessDate("BUSINESS_DATE", "2026-01-15");
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(2000), "05 January 2026"));

            // Both repayments should be reflected — balance math is order-independent
            GetWorkingCapitalLoansLoanIdResponse afterBackdated = wcLoanHelper.getLoanDetails(loanId);
            assertNotNull(afterBackdated.getBalance(), "Balance should exist after backdated repayment");
            assertEqualBigDecimal(BigDecimal.valueOf(4000), afterBackdated.getBalance().getPrincipalOutstanding(),
                    "Outstanding should be 4000 after total 5000 repaid on 9000 loan");
            assertEqualBigDecimal(BigDecimal.valueOf(5000), afterBackdated.getBalance().getPrincipalPaid(),
                    "Principal paid should be 5000 (2000 + 3000)");

            // Both repayments fit into principal: the backdated one allocates fully, the earlier one stays untouched
            List<GetWorkingCapitalLoanTransactionIdResponse> transactions = wcLoanHelper.getTransactions(loanId);
            assertAllocation(findTransaction(transactions, LocalDate.of(2026, 1, 5), BigDecimal.valueOf(2000)), BigDecimal.valueOf(2000));
            assertAllocation(findTransaction(transactions, LocalDate.of(2026, 1, 10), BigDecimal.valueOf(3000)), BigDecimal.valueOf(3000));
        });
    }

    @Test
    void testBackdatedRepayment_excessBecomesOverpayment() {
        businessDateHelper.runAt("2026-01-01", () -> {
            Long clientForTest = clientHelper.createClient("01 January 2026");
            Long loanId = createAndDisburseLoanOnDate(clientForTest, BigDecimal.valueOf(9000), "01 January 2026");

            // Partial repayment on day 10 (loan stays ACTIVE with 2000 outstanding)
            businessDateHelper.updateBusinessDate("BUSINESS_DATE", "2026-01-10");
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(7000), "10 January 2026"));

            GetWorkingCapitalLoansLoanIdResponse afterRepayment = wcLoanHelper.getLoanDetails(loanId);
            assertNotNull(afterRepayment.getBalance(), "Balance should exist after repayment");
            assertEqualBigDecimal(BigDecimal.valueOf(2000), afterRepayment.getBalance().getPrincipalOutstanding(),
                    "Outstanding should be 2000 after 7000 repayment on 9000 loan");

            // Backdated repayment on day 5 — total repaid (5000 + 7000 = 12000) exceeds principal (9000)
            businessDateHelper.updateBusinessDate("BUSINESS_DATE", "2026-01-15");
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(5000), "05 January 2026"));

            // Totals are order-independent: 9000 principal repaid, 3000 overpayment
            GetWorkingCapitalLoansLoanIdResponse afterBackdated = wcLoanHelper.getLoanDetails(loanId);
            assertNotNull(afterBackdated.getBalance(), "Balance should exist after backdated repayment");
            assertEqualBigDecimal(BigDecimal.ZERO, afterBackdated.getBalance().getPrincipalOutstanding(),
                    "Outstanding should be 0 — principal is fully repaid");
            assertEqualBigDecimal(BigDecimal.valueOf(9000), afterBackdated.getBalance().getPrincipalPaid(),
                    "Principal paid should be 9000 — capped at total principal");
            assertEqualBigDecimal(BigDecimal.valueOf(3000), afterBackdated.getBalance().getOverpaymentAmount(),
                    "Overpayment should be 3000 (5000 + 7000 - 9000 principal)");

            // Without charges, allocations are not redistributed: the day-10 repayment keeps its original
            // 7000 principal allocation, and the backdated day-5 repayment allocates against the 2000 that
            // was outstanding when it was booked (its excess 3000 is overpayment, not part of the allocation).
            List<GetWorkingCapitalLoanTransactionIdResponse> transactions = wcLoanHelper.getTransactions(loanId);
            assertAllocation(findTransaction(transactions, LocalDate.of(2026, 1, 5), BigDecimal.valueOf(5000)), BigDecimal.valueOf(2000));
            assertAllocation(findTransaction(transactions, LocalDate.of(2026, 1, 10), BigDecimal.valueOf(7000)), BigDecimal.valueOf(7000));
        });
    }

    @Test
    void testMultipleBackdatedRepaymentsAccumulateCorrectly() {
        businessDateHelper.runAt("2026-01-01", () -> {
            Long clientForTest = clientHelper.createClient("01 January 2026");
            Long loanId = createAndDisburseLoanOnDate(clientForTest, BigDecimal.valueOf(9000), "01 January 2026");

            // First repayment on day 15
            businessDateHelper.updateBusinessDate("BUSINESS_DATE", "2026-01-15");
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(3000), "15 January 2026"));

            GetWorkingCapitalLoansLoanIdResponse afterFirst = wcLoanHelper.getLoanDetails(loanId);
            assertNotNull(afterFirst.getBalance());
            assertEqualBigDecimal(BigDecimal.valueOf(6000), afterFirst.getBalance().getPrincipalOutstanding(),
                    "Outstanding should be 6000 after first repayment");

            // Backdated repayment on day 5
            businessDateHelper.updateBusinessDate("BUSINESS_DATE", "2026-01-20");
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(1000), "05 January 2026"));

            GetWorkingCapitalLoansLoanIdResponse afterSecond = wcLoanHelper.getLoanDetails(loanId);
            assertNotNull(afterSecond.getBalance());
            assertEqualBigDecimal(BigDecimal.valueOf(5000), afterSecond.getBalance().getPrincipalOutstanding(),
                    "Outstanding should be 5000 after 4000 total repaid");
            assertEqualBigDecimal(BigDecimal.valueOf(4000), afterSecond.getBalance().getPrincipalPaid(),
                    "Principal paid should be 4000 (1000 + 3000)");

            // Another backdated repayment on day 10 (between existing ones)
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(2000), "10 January 2026"));

            GetWorkingCapitalLoansLoanIdResponse afterThird = wcLoanHelper.getLoanDetails(loanId);
            assertNotNull(afterThird.getBalance());
            assertEqualBigDecimal(BigDecimal.valueOf(3000), afterThird.getBalance().getPrincipalOutstanding(),
                    "Outstanding should be 3000 after 6000 total repaid");
            assertEqualBigDecimal(BigDecimal.valueOf(6000), afterThird.getBalance().getPrincipalPaid(),
                    "Principal paid should be 6000 (1000 + 2000 + 3000)");
        });
    }

    @Test
    void testNonBackdatedRepaymentDoesNotTriggerReprocessing() {
        businessDateHelper.runAt("2026-01-01", () -> {
            Long clientForTest = clientHelper.createClient("01 January 2026");
            Long loanId = createAndDisburseLoanOnDate(clientForTest, BigDecimal.valueOf(9000), "01 January 2026");

            // Sequential repayments (not backdated — each on or after the business date)
            businessDateHelper.updateBusinessDate("BUSINESS_DATE", "2026-01-05");
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(2000), "05 January 2026"));

            businessDateHelper.updateBusinessDate("BUSINESS_DATE", "2026-01-10");
            wcLoanHelper.makeRepayment(loanId, WorkingCapitalLoanRequestBuilders.repayment(BigDecimal.valueOf(3000), "10 January 2026"));

            // Verify balance is the simple sum — no reprocessing side effects
            GetWorkingCapitalLoansLoanIdResponse loan = wcLoanHelper.getLoanDetails(loanId);
            assertNotNull(loan.getBalance());
            assertEqualBigDecimal(BigDecimal.valueOf(4000), loan.getBalance().getPrincipalOutstanding(),
                    "Outstanding should be 4000 after sequential 2000 + 3000 repayments");
            assertEqualBigDecimal(BigDecimal.valueOf(5000), loan.getBalance().getPrincipalPaid(),
                    "Principal paid should be 5000 after sequential repayments");
            assertEqualBigDecimal(BigDecimal.ZERO, loan.getBalance().getOverpaymentAmount(),
                    "No overpayment expected for sequential repayments under principal");
        });
    }

    private Long createAndDisburseLoanOnDate(Long clientIdParam, BigDecimal principal, String date) {
        Long productId = createProduct();
        Long loanId = submitAndTrack(clientIdParam, productId, principal, date);
        wcLoanHelper.approve(loanId, WorkingCapitalLoanRequestBuilders.approve(date, principal, date));
        wcLoanHelper.disburse(loanId, WorkingCapitalLoanRequestBuilders.disburse(date, principal));
        return loanId;
    }

    private Long submitAndTrack(Long clientIdParam, Long productId, BigDecimal principal, String date) {
        Long loanId = wcLoanHelper.submitApplication(WorkingCapitalLoanRequestBuilders.submitApplication(clientIdParam, productId,
                principal, BigDecimal.valueOf(18), date, date));
        createdLoanIds.add(loanId);
        return loanId;
    }

    private Long createProduct() {
        String uniqueName = "WCL Reprocess " + Utils.uniqueRandomStringGenerator("", 8);
        String uniqueShortName = Utils.uniqueRandomStringGenerator("", 4);
        Long productId = productHelper
                .createWorkingCapitalLoanProduct(
                        new WorkingCapitalLoanProductTestBuilder().withName(uniqueName).withShortName(uniqueShortName).build())
                .getResourceId();
        createdProductIds.add(productId);
        return productId;
    }

    private static GetWorkingCapitalLoanTransactionIdResponse findTransaction(List<GetWorkingCapitalLoanTransactionIdResponse> transactions,
            LocalDate transactionDate, BigDecimal amount) {
        return transactions.stream().filter(txn -> transactionDate.equals(txn.getTransactionDate()))
                .filter(txn -> txn.getTransactionAmount() != null && amount.compareTo(txn.getTransactionAmount()) == 0).findFirst()
                .orElseThrow(() -> new AssertionError("Transaction not found on " + transactionDate + " with amount " + amount));
    }

    private static void assertAllocation(GetWorkingCapitalLoanTransactionIdResponse transaction, BigDecimal expectedPrincipalPortion) {
        String context = "Transaction on " + transaction.getTransactionDate() + " amount " + transaction.getTransactionAmount();
        assertEqualBigDecimal(expectedPrincipalPortion, transaction.getPrincipalPortion(), context + " — principal portion");
        assertEqualBigDecimal(BigDecimal.ZERO, transaction.getFeeChargesPortion(), context + " — fee charges portion");
        assertEqualBigDecimal(BigDecimal.ZERO, transaction.getPenaltyChargesPortion(), context + " — penalty charges portion");
    }

    private static void assertEqualBigDecimal(BigDecimal expected, BigDecimal actual, String message) {
        assertNotNull(actual, message + " — value was null");
        assertEquals(0, expected.compareTo(actual), message + " — expected: " + expected + " but was: " + actual);
    }
}
