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

import static org.apache.fineract.client.feign.util.FeignCalls.fail;
import static org.apache.fineract.client.feign.util.FeignCalls.ok;
import static org.assertj.core.api.Assertions.assertThat;

import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.feign.util.CallFailedRuntimeException;
import org.apache.fineract.client.models.PostWorkingCapitalLoansBreachActionRequest;
import org.apache.fineract.client.models.PostWorkingCapitalLoansBreachActionResponse;
import org.apache.fineract.client.models.PostWorkingCapitalLoansResponse;
import org.apache.fineract.client.models.WorkingCapitalLoanBreachActionData;
import org.apache.fineract.test.factory.WorkingCapitalLoanRequestFactory;
import org.apache.fineract.test.stepdef.AbstractStepDef;
import org.apache.fineract.test.support.TestContextKey;

@Slf4j
@RequiredArgsConstructor
public class WorkingCapitalBreachActionStepDef extends AbstractStepDef {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("dd MMMM yyyy");

    private final FineractFeignClient fineractFeignClient;
    private final WorkingCapitalLoanRequestFactory workingCapitalLoanRequestFactory;

    @When("Admin creates WC breach reschedule action with the following parameters:")
    public void createRescheduleAction(final DataTable table) {
        final Map<String, String> params = table.asMaps().getFirst();
        final PostWorkingCapitalLoansBreachActionRequest request = buildRescheduleRequest(params);
        executeRescheduleAction(request);
    }

    @Then("Admin fails to create WC breach reschedule action with minimumPayment {int} {word} and frequency {int} {word} with error containing {string}")
    public void failToCreateRescheduleActionWithMessage(final int minimumPayment, final String minimumPaymentType, final int frequency,
            final String frequencyType, final String expectedMessage) {
        final Long loanId = getLoanId();
        final PostWorkingCapitalLoansBreachActionRequest request = buildRescheduleRequest(new BigDecimal(minimumPayment),
                minimumPaymentType, frequency, frequencyType);
        final CallFailedRuntimeException exception = fail(
                () -> fineractFeignClient.workingCapitalLoanBreachActions().createBreachAction(loanId, request));
        assertThat(exception.getStatus()).as("HTTP status code").isEqualTo(400);
        assertThat(exception.getDeveloperMessage()).as("Developer message").contains(expectedMessage);
    }

    @Then("Admin fails to create WC breach reschedule action with no parameters with error containing {string}")
    public void failToCreateEmptyRescheduleAction(final String expectedMessage) {
        final Long loanId = getLoanId();
        final PostWorkingCapitalLoansBreachActionRequest request = buildRescheduleRequest(Map.of());

        final CallFailedRuntimeException exception = fail(
                () -> fineractFeignClient.workingCapitalLoanBreachActions().createBreachAction(loanId, request));
        assertThat(exception.getStatus()).as("HTTP status code").isEqualTo(400);
        assertThat(exception.getDeveloperMessage()).as("Developer message").contains(expectedMessage);
    }

    @Then("WC loan breach actions have the following data:")
    public void verifyBreachActionsHistory(final DataTable table) {
        final Long loanId = getLoanId();
        final List<WorkingCapitalLoanBreachActionData> actions = retrieveBreachActions(loanId);
        final List<Map<String, String>> expectedRows = table.asMaps();
        assertThat(actions).as("Breach actions count").hasSize(expectedRows.size());
        for (int i = 0; i < expectedRows.size(); i++) {
            final WorkingCapitalLoanBreachActionData actual = actions.get(i);
            final int rowNumber = i + 1;
            expectedRows.get(i).forEach((field, value) -> verifyActionField(actual, field, value, rowNumber));
        }
        log.info("Successfully verified {} breach action(s) for loan {}", actions.size(), loanId);
    }

    private void executeRescheduleAction(final PostWorkingCapitalLoansBreachActionRequest request) {
        final Long loanId = getLoanId();
        log.debug("Creating breach RESCHEDULE action for WC loan {}: {}", loanId, request);

        final PostWorkingCapitalLoansBreachActionResponse result = ok(
                () -> fineractFeignClient.workingCapitalLoanBreachActions().createBreachAction(loanId, request));
        assertThat(result).isNotNull();
        assertThat(result.getResourceId()).isNotNull();
        log.info("Breach RESCHEDULE action created with id={}", result.getResourceId());
    }

    private List<WorkingCapitalLoanBreachActionData> retrieveBreachActions(final Long loanId) {
        return ok(() -> fineractFeignClient.workingCapitalLoanBreachActions().retrieveBreachActions(loanId));
    }

    private void verifyActionField(final WorkingCapitalLoanBreachActionData actual, final String field, final String expected,
            final int rowNumber) {
        final String label = "Action " + rowNumber + " " + field;
        switch (field) {
            case "action" -> {
                assert actual.getAction() != null;
                assertThat(actual.getAction().name()).as(label).isEqualTo(expected);
            }
            case "startDate" -> assertThat(actual.getStartDate()).as(label).isEqualTo(LocalDate.parse(expected, DATE_FORMAT));
            case "minimumPayment" -> assertThat(actual.getMinimumPayment()).as(label).isEqualByComparingTo(new BigDecimal(expected));
            case "minimumPaymentType" ->
                verifyOptionalField(expected, v -> assertThat(String.valueOf(actual.getMinimumPaymentType())).as(label).isEqualTo(v),
                        () -> assertThat(actual.getMinimumPaymentType()).as(label).isNull());
            case "frequency" -> assertThat(actual.getFrequency()).as(label).isEqualTo(Integer.parseInt(expected));
            case "frequencyType" ->
                verifyOptionalField(expected, v -> assertThat(String.valueOf(actual.getFrequencyType())).as(label).isEqualTo(v),
                        () -> assertThat(actual.getFrequencyType()).as(label).isNull());
            default -> throw new IllegalArgumentException("Unknown action field: " + field);
        }
    }

    private void verifyOptionalField(final String expected, final Consumer<String> whenPresent, final Runnable whenAbsent) {
        Optional.ofNullable(expected).filter(Predicate.not(String::isBlank)).ifPresentOrElse(whenPresent, whenAbsent);
    }

    private Long getLoanId() {
        final PostWorkingCapitalLoansResponse loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        assertThat(loanResponse).isNotNull();
        return loanResponse.getLoanId();
    }

    private PostWorkingCapitalLoansBreachActionRequest buildRescheduleRequest(final BigDecimal minimumPayment,
            final String minimumPaymentType, final int frequency, final String frequencyType) {
        return buildRescheduleRequest(Map.of("minimumPayment", minimumPayment.toPlainString(), "minimumPaymentType", minimumPaymentType,
                "frequency", String.valueOf(frequency), "frequencyType", frequencyType));
    }

    private PostWorkingCapitalLoansBreachActionRequest buildRescheduleRequest(final Map<String, String> params) {
        final PostWorkingCapitalLoansBreachActionRequest request = workingCapitalLoanRequestFactory
                .defaultWorkingCapitalLoansBreachActionRequest("reschedule");
        Optional.ofNullable(params.get("minimumPayment")).ifPresent(v -> request.setMinimumPayment(new BigDecimal(v)));
        Optional.ofNullable(params.get("minimumPaymentType")).ifPresent(request::setMinimumPaymentType);
        Optional.ofNullable(params.get("frequency")).ifPresent(v -> request.setFrequency(Integer.parseInt(v)));
        Optional.ofNullable(params.get("frequencyType")).ifPresent(request::setFrequencyType);
        return request;
    }
}
