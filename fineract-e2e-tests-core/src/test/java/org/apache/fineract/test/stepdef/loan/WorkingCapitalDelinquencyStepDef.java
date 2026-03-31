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

import static org.apache.fineract.client.feign.util.FeignCalls.ok;
import static org.assertj.core.api.Assertions.assertThat;

import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Then;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.models.PostWorkingCapitalLoansResponse;
import org.apache.fineract.client.models.WorkingCapitalLoanDelinquencyRangeScheduleData;
import org.apache.fineract.test.stepdef.AbstractStepDef;
import org.apache.fineract.test.support.TestContextKey;

@Slf4j
@RequiredArgsConstructor
public class WorkingCapitalDelinquencyStepDef extends AbstractStepDef {

    private final FineractFeignClient fineractClient;

    @Then("Working Capital loan delinquency range schedule has no data on a not yet disbursed loan")
    public void verifyRangeScheduleIsEmpty() {
        Long loanId = extractLoanId();
        List<WorkingCapitalLoanDelinquencyRangeScheduleData> actualRangeSchedule = retrieveRangeSchedule(loanId);

        assertThat(actualRangeSchedule).as("Range schedule should be empty when loan is not yet disbursed").isEmpty();

        log.info("Verified that loan {} has no delinquency range schedule on a not yet disbursed loan", loanId);
    }

    @Then("Working Capital loan delinquency range schedule has the following data:")
    public void verifyRangeSchedule(DataTable dataTable) {
        Long loanId = extractLoanId();
        List<WorkingCapitalLoanDelinquencyRangeScheduleData> actualRangeSchedule = retrieveRangeSchedule(loanId);

        // If no data rows provided (only header), just log and return
        if (dataTable.height() <= 1) {
            log.info("No expected data provided for verification, skipping validation");
            return;
        }

        List<List<String>> rows = dataTable.asLists();
        List<String> headers = rows.get(0);
        List<List<String>> expectedData = rows.subList(1, rows.size());

        verifyRangeScheduleSize(actualRangeSchedule, expectedData.size());
        verifyAllRangeScheduleFields(actualRangeSchedule, headers, expectedData);

        log.info("Successfully verified {} range schedule entries", actualRangeSchedule.size());
    }

    private Long extractLoanId() {
        final PostWorkingCapitalLoansResponse loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        return loanResponse.getLoanId();
    }

    private List<WorkingCapitalLoanDelinquencyRangeScheduleData> retrieveRangeSchedule(Long loanId) {
        List<WorkingCapitalLoanDelinquencyRangeScheduleData> rangeSchedule = ok(
                () -> fineractClient.workingCapitalLoanDelinquencyRangeSchedule().retrieveDelinquencyRangeSchedule(loanId));
        log.debug("Delinquency Range Schedule for loan {}: {}", loanId, rangeSchedule);
        return rangeSchedule;
    }

    private void verifyRangeScheduleSize(List<WorkingCapitalLoanDelinquencyRangeScheduleData> actualRangeSchedule, int expectedSize) {
        assertThat(actualRangeSchedule).as("Range schedule size should match expected data").hasSize(expectedSize);
    }

    private void verifyAllRangeScheduleFields(List<WorkingCapitalLoanDelinquencyRangeScheduleData> actualRangeSchedule,
            List<String> headers, List<List<String>> expectedData) {
        for (int i = 0; i < expectedData.size(); i++) {
            List<String> expectedRow = expectedData.get(i);
            WorkingCapitalLoanDelinquencyRangeScheduleData actualRow = actualRangeSchedule.get(i);

            for (int j = 0; j < headers.size(); j++) {
                String header = headers.get(j);
                String expectedValue = expectedRow.get(j);
                verifyRangeScheduleField(actualRow, header, expectedValue, i + 1);
            }
        }
    }

    private void verifyRangeScheduleField(WorkingCapitalLoanDelinquencyRangeScheduleData actual, String fieldName, String expectedValue,
            int rowNumber) {
        switch (fieldName) {
            case "periodNumber" ->
                assertThat(actual.getPeriodNumber()).as("Period number for row %d", rowNumber).isEqualTo(Integer.parseInt(expectedValue));
            case "fromDate" ->
                assertThat(actual.getFromDate()).as("From date for row %d", rowNumber).isEqualTo(LocalDate.parse(expectedValue));
            case "toDate" -> assertThat(actual.getToDate()).as("To date for row %d", rowNumber).isEqualTo(LocalDate.parse(expectedValue));
            case "expectedAmount" -> assertThat(actual.getExpectedAmount()).as("Expected amount for row %d", rowNumber)
                    .isEqualByComparingTo(new BigDecimal(expectedValue));
            case "paidAmount" -> assertThat(actual.getPaidAmount()).as("Paid amount for row %d", rowNumber)
                    .isEqualByComparingTo(new BigDecimal(expectedValue));
            case "outstandingAmount" -> assertThat(actual.getOutstandingAmount()).as("Outstanding amount for row %d", rowNumber)
                    .isEqualByComparingTo(new BigDecimal(expectedValue));
            case "minPaymentCriteriaMet" ->
                verifyNullableBoolean(actual.getMinPaymentCriteriaMet(), expectedValue, "Min payment criteria met", rowNumber);
            case "delinquentAmount" ->
                verifyNullableBigDecimal(actual.getDelinquentAmount(), expectedValue, "Delinquent amount", rowNumber);
            case "delinquentDays" -> verifyNullableLong(actual.getDelinquentDays(), expectedValue, "Delinquent days", rowNumber);
            default -> throw new IllegalArgumentException("Unknown field name: " + fieldName);
        }
    }

    private void verifyNullableBoolean(Boolean actualValue, String expectedValue, String fieldDescription, int rowNumber) {
        if ("null".equals(expectedValue)) {
            assertThat(actualValue).as("%s for row %d", fieldDescription, rowNumber).isNull();
        } else {
            assertThat(actualValue).as("%s for row %d", fieldDescription, rowNumber).isEqualTo(Boolean.parseBoolean(expectedValue));
        }
    }

    private void verifyNullableBigDecimal(BigDecimal actualValue, String expectedValue, String fieldDescription, int rowNumber) {
        if ("null".equals(expectedValue)) {
            assertThat(actualValue).as("%s for row %d", fieldDescription, rowNumber).isNull();
        } else {
            assertThat(actualValue).as("%s for row %d", fieldDescription, rowNumber).isEqualByComparingTo(new BigDecimal(expectedValue));
        }
    }

    private void verifyNullableLong(Long actualValue, String expectedValue, String fieldDescription, int rowNumber) {
        if ("null".equals(expectedValue)) {
            assertThat(actualValue).as("%s for row %d", fieldDescription, rowNumber).isNull();
        } else {
            assertThat(actualValue).as("%s for row %d", fieldDescription, rowNumber).isEqualTo(Long.parseLong(expectedValue));
        }
    }

}
