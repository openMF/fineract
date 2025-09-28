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
package org.apache.fineract.test.stepdef.loan;

import static org.assertj.core.api.Assertions.assertThat;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.models.LoanScheduleData;
import org.apache.fineract.client.models.LoanSchedulePeriodData;
import org.apache.fineract.client.models.PostLoansLoanIdTransactionsRequest;
import org.apache.fineract.client.models.PostLoansLoanIdTransactionsResponse;
import org.apache.fineract.client.models.PostLoansResponse;
import org.apache.fineract.client.services.LoanTransactionsApi;
import org.apache.fineract.test.factory.LoanRequestFactory;
import org.apache.fineract.test.helper.ErrorHelper;
import org.apache.fineract.test.helper.ErrorMessageHelper;
import org.apache.fineract.test.helper.Utils;
import org.apache.fineract.test.messaging.EventAssertion;
import org.apache.fineract.test.messaging.event.loan.LoanReAgeEvent;
import org.apache.fineract.test.stepdef.AbstractStepDef;
import org.apache.fineract.test.support.TestContextKey;
import org.springframework.beans.factory.annotation.Autowired;
import retrofit2.Response;

@Slf4j
public class LoanReAgingStepDef extends AbstractStepDef {

    private static final String DATE_FORMAT = "dd MMMM yyyy";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT);

    @Autowired
    private LoanTransactionsApi loanTransactionsApi;

    @Autowired
    private EventAssertion eventAssertion;

    @When("Admin creates a Loan re-aging transaction with the following data:")
    public void createReAgingTransaction(DataTable table) throws IOException {
        Response<PostLoansResponse> loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        long loanId = loanResponse.body().getLoanId();

        List<String> data = table.asLists().get(1);
        int frequencyNumber = Integer.parseInt(data.get(0));
        String frequencyType = data.get(1);
        String startDate = data.get(2);
        int numberOfInstallments = Integer.parseInt(data.get(3));

        PostLoansLoanIdTransactionsRequest reAgingRequest = LoanRequestFactory//
                .defaultReAgingRequest()//
                .frequencyNumber(frequencyNumber)//
                .frequencyType(frequencyType)//
                .startDate(startDate)//
                .numberOfInstallments(numberOfInstallments);//

        Response<PostLoansLoanIdTransactionsResponse> response = loanTransactionsApi.executeLoanTransaction(loanId, reAgingRequest, "reAge")
                .execute();
        ErrorHelper.checkSuccessfulApiCall(response);
        testContext().set(TestContextKey.LOAN_REAGING_RESPONSE, response);
    }

    @When("Admin creates a Loan re-aging transaction by Loan external ID with the following data:")
    public void createReAgingTransactionByLoanExternalId(DataTable table) throws IOException {
        Response<PostLoansResponse> loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        String loanExternalId = loanResponse.body().getResourceExternalId();

        List<String> data = table.asLists().get(1);
        int frequencyNumber = Integer.parseInt(data.get(0));
        String frequencyType = data.get(1);
        String startDate = data.get(2);
        int numberOfInstallments = Integer.parseInt(data.get(3));

        PostLoansLoanIdTransactionsRequest reAgingRequest = LoanRequestFactory//
                .defaultReAgingRequest()//
                .frequencyNumber(frequencyNumber)//
                .frequencyType(frequencyType)//
                .startDate(startDate)//
                .numberOfInstallments(numberOfInstallments);//

        Response<PostLoansLoanIdTransactionsResponse> response = loanTransactionsApi
                .executeLoanTransaction1(loanExternalId, reAgingRequest, "reAge").execute();
        ErrorHelper.checkSuccessfulApiCall(response);
        testContext().set(TestContextKey.LOAN_REAGING_RESPONSE, response);
    }

    @When("Admin successfully undo Loan re-aging transaction")
    public void undoReAgingTransaction() throws IOException {
        Response<PostLoansResponse> loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        long loanId = loanResponse.body().getLoanId();

        Response<PostLoansLoanIdTransactionsResponse> response = loanTransactionsApi
                .executeLoanTransaction(loanId, new PostLoansLoanIdTransactionsRequest(), "undoReAge").execute();
        ErrorHelper.checkSuccessfulApiCall(response);
        testContext().set(TestContextKey.LOAN_REAGING_UNDO_RESPONSE, response);
    }

    @Then("LoanReAgeBusinessEvent is created")
    public void checkLoanReAmortizeBusinessEventCreated() {
        Response<PostLoansResponse> loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        long loanId = loanResponse.body().getLoanId();

        eventAssertion.assertEventRaised(LoanReAgeEvent.class, loanId);
    }

    @When("Admin creates a Loan re-aging preview with the following data:")
    public void createReAgingPreview(DataTable table) throws IOException {
        Response<PostLoansResponse> loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        long loanId = loanResponse.body().getLoanId();

        List<String> data = table.asLists().get(1);
        int frequencyNumber = Integer.parseInt(data.get(0));
        String frequencyType = data.get(1);
        String startDate = data.get(2);
        int numberOfInstallments = Integer.parseInt(data.get(3));

        PostLoansLoanIdTransactionsRequest reAgingRequest = LoanRequestFactory//
                .defaultReAgingRequest()//
                .frequencyNumber(frequencyNumber)//
                .frequencyType(frequencyType)//
                .startDate(startDate)//
                .numberOfInstallments(numberOfInstallments);//

        Response<LoanScheduleData> response = loanTransactionsApi.previewReAgeSchedule(loanId, reAgingRequest).execute();
        ErrorHelper.checkSuccessfulApiCall(response);
        testContext().set(TestContextKey.LOAN_REAGING_PREVIEW_RESPONSE, response);

        log.info(
                "Re-aging preview created for loan ID: {} with parameters: frequencyNumber={}, frequencyType={}, startDate={}, numberOfInstallments={}",
                loanId, frequencyNumber, frequencyType, startDate, numberOfInstallments);
    }

    @When("Admin creates a Loan re-aging preview by Loan external ID with the following data:")
    public void createReAgingPreviewByLoanExternalId(DataTable table) throws IOException {
        Response<PostLoansResponse> loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        String loanExternalId = loanResponse.body().getResourceExternalId();

        List<String> data = table.asLists().get(1);
        int frequencyNumber = Integer.parseInt(data.get(0));
        String frequencyType = data.get(1);
        String startDate = data.get(2);
        int numberOfInstallments = Integer.parseInt(data.get(3));

        PostLoansLoanIdTransactionsRequest reAgingRequest = LoanRequestFactory//
                .defaultReAgingRequest()//
                .frequencyNumber(frequencyNumber)//
                .frequencyType(frequencyType)//
                .startDate(startDate)//
                .numberOfInstallments(numberOfInstallments);//

        Response<LoanScheduleData> response = loanTransactionsApi.previewReAgeSchedule1(loanExternalId, reAgingRequest).execute();
        ErrorHelper.checkSuccessfulApiCall(response);
        testContext().set(TestContextKey.LOAN_REAGING_PREVIEW_RESPONSE, response);

        log.info(
                "Re-aging preview created for loan external ID: {} with parameters: frequencyNumber={}, frequencyType={}, startDate={}, numberOfInstallments={}",
                loanExternalId, frequencyNumber, frequencyType, startDate, numberOfInstallments);
    }

    @Then("Loan Repayment schedule preview has {int} periods, with the following data for periods:")
    public void loanRepaymentSchedulePreviewPeriodsCheck(int linesExpected, DataTable table) {
        Response<LoanScheduleData> scheduleResponse = testContext().get(TestContextKey.LOAN_REAGING_PREVIEW_RESPONSE);

        List<LoanSchedulePeriodData> repaymentPeriods = scheduleResponse.body().getPeriods();

        Response<PostLoansResponse> loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        String resourceId = String.valueOf(loanResponse.body().getLoanId());

        List<List<String>> data = table.asLists();
        int nrLines = data.size();
        int linesActual = (int) repaymentPeriods.stream().filter(r -> r.getPeriod() != null).count();
        for (int i = 1; i < nrLines; i++) {
            List<String> expectedValues = data.get(i);
            String dueDateExpected = expectedValues.get(2);

            List<List<String>> actualValuesList = repaymentPeriods.stream()
                    .filter(r -> dueDateExpected.equals(FORMATTER.format(r.getDueDate())))
                    .map(r -> fetchValuesOfRepaymentSchedule(data.get(0), r)).collect(Collectors.toList());

            boolean containsExpectedValues = actualValuesList.stream().anyMatch(actualValues -> actualValues.equals(expectedValues));
            assertThat(containsExpectedValues)
                    .as(ErrorMessageHelper.wrongValueInLineInRepaymentSchedule(resourceId, i, actualValuesList, expectedValues)).isTrue();

            assertThat(linesActual).as(ErrorMessageHelper.wrongNumberOfLinesInRepaymentSchedule(resourceId, linesActual, linesExpected))
                    .isEqualTo(linesExpected);
        }
    }

    @Then("Loan Repayment schedule preview has the following data in Total row:")
    public void loanRepaymentScheduleAmountCheck(DataTable table) {
        List<List<String>> data = table.asLists();
        List<String> header = data.get(0);
        List<String> expectedValues = data.get(1);
        Response<LoanScheduleData> scheduleResponse = testContext().get(TestContextKey.LOAN_REAGING_PREVIEW_RESPONSE);
        validateRepaymentScheduleTotal(header, scheduleResponse.body(), expectedValues);
    }

    private List<String> fetchValuesOfRepaymentSchedule(List<String> header, LoanSchedulePeriodData repaymentPeriod) {
        List<String> actualValues = new ArrayList<>();
        for (String headerName : header) {
            switch (headerName) {
                case "Nr" -> actualValues.add(repaymentPeriod.getPeriod() == null ? null : String.valueOf(repaymentPeriod.getPeriod()));
                case "Days" ->
                    actualValues.add(repaymentPeriod.getDaysInPeriod() == null ? null : String.valueOf(repaymentPeriod.getDaysInPeriod()));
                case "Date" ->
                    actualValues.add(repaymentPeriod.getDueDate() == null ? null : FORMATTER.format(repaymentPeriod.getDueDate()));
                case "Paid date" -> actualValues.add(repaymentPeriod.getObligationsMetOnDate() == null ? null
                        : FORMATTER.format(repaymentPeriod.getObligationsMetOnDate()));
                case "Balance of loan" -> actualValues.add(repaymentPeriod.getPrincipalLoanBalanceOutstanding() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getPrincipalLoanBalanceOutstanding().doubleValue()).format());
                case "Principal due" -> actualValues.add(repaymentPeriod.getPrincipalDue() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getPrincipalDue().doubleValue()).format());
                case "Interest" -> actualValues.add(repaymentPeriod.getInterestDue() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getInterestDue().doubleValue()).format());
                case "Fees" -> actualValues.add(repaymentPeriod.getFeeChargesDue() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getFeeChargesDue().doubleValue()).format());
                case "Penalties" -> actualValues.add(repaymentPeriod.getPenaltyChargesDue() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getPenaltyChargesDue().doubleValue()).format());
                case "Due" -> actualValues.add(repaymentPeriod.getTotalDueForPeriod() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getTotalDueForPeriod().doubleValue()).format());
                case "Paid" -> actualValues.add(repaymentPeriod.getTotalPaidForPeriod() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getTotalPaidForPeriod().doubleValue()).format());
                case "In advance" -> actualValues.add(repaymentPeriod.getTotalPaidInAdvanceForPeriod() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getTotalPaidInAdvanceForPeriod().doubleValue()).format());
                case "Late" -> actualValues.add(repaymentPeriod.getTotalPaidLateForPeriod() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getTotalPaidLateForPeriod().doubleValue()).format());
                case "Waived" -> actualValues.add(repaymentPeriod.getTotalWaivedForPeriod() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getTotalWaivedForPeriod().doubleValue()).format());
                case "Outstanding" -> actualValues.add(repaymentPeriod.getTotalOutstandingForPeriod() == null ? null
                        : new Utils.DoubleFormatter(repaymentPeriod.getTotalOutstandingForPeriod().doubleValue()).format());
                default -> throw new IllegalStateException(String.format("Header name %s cannot be found", headerName));
            }
        }
        return actualValues;
    }

    @SuppressFBWarnings("SF_SWITCH_NO_DEFAULT")
    private List<String> validateRepaymentScheduleTotal(List<String> header, LoanScheduleData repaymentSchedule,
            List<String> expectedAmounts) {
        List<String> actualValues = new ArrayList<>();
        Double paidActual = 0.0;
        List<LoanSchedulePeriodData> periods = repaymentSchedule.getPeriods();
        for (LoanSchedulePeriodData period : periods) {
            if (null != period.getTotalPaidForPeriod()) {
                paidActual += period.getTotalPaidForPeriod().doubleValue();
            }
        }
        BigDecimal paidActualBd = new BigDecimal(paidActual).setScale(2, RoundingMode.HALF_DOWN);

        for (int i = 0; i < header.size(); i++) {
            String headerName = header.get(i);
            String expectedValue = expectedAmounts.get(i);
            switch (headerName) {
                case "Principal due" -> assertThat(repaymentSchedule.getTotalPrincipalExpected().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentSchedulePrincipal(
                                repaymentSchedule.getTotalPrincipalExpected().doubleValue(), Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "Interest" -> assertThat(repaymentSchedule.getTotalInterestCharged().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentScheduleInterest(
                                repaymentSchedule.getTotalInterestCharged().doubleValue(), Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "Fees" -> assertThat(repaymentSchedule.getTotalFeeChargesCharged().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentScheduleFees(
                                repaymentSchedule.getTotalFeeChargesCharged().doubleValue(), Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "Penalties" -> assertThat(repaymentSchedule.getTotalPenaltyChargesCharged().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentSchedulePenalties(
                                repaymentSchedule.getTotalPenaltyChargesCharged().doubleValue(), Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "Due" -> assertThat(repaymentSchedule.getTotalRepaymentExpected().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentScheduleDue(
                                repaymentSchedule.getTotalRepaymentExpected().doubleValue(), Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "Paid" -> assertThat(paidActualBd.doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentSchedulePaid(paidActualBd.doubleValue(),
                                Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "In advance" -> assertThat(repaymentSchedule.getTotalPaidInAdvance().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentScheduleInAdvance(
                                repaymentSchedule.getTotalPaidInAdvance().doubleValue(), Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "Late" -> assertThat(repaymentSchedule.getTotalPaidLate().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentScheduleLate(repaymentSchedule.getTotalPaidLate().doubleValue(),
                                Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "Waived" -> assertThat(repaymentSchedule.getTotalWaived().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentScheduleWaived(repaymentSchedule.getTotalWaived().doubleValue(),
                                Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
                case "Outstanding" -> assertThat(repaymentSchedule.getTotalOutstanding().doubleValue())//
                        .as(ErrorMessageHelper.wrongAmountInRepaymentScheduleOutstanding(
                                repaymentSchedule.getTotalOutstanding().doubleValue(), Double.valueOf(expectedValue)))//
                        .isEqualTo(Double.valueOf(expectedValue));//
            }
        }
        return actualValues;
    }

}
