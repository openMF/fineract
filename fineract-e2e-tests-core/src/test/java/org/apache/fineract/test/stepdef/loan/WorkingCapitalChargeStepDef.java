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
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.feign.util.CallFailedRuntimeException;
import org.apache.fineract.client.models.ChargeData;
import org.apache.fineract.client.models.ChargeRequest;
import org.apache.fineract.client.models.EnumOptionData;
import org.apache.fineract.client.models.GetChargesResponse;
import org.apache.fineract.client.models.PostChargesResponse;
import org.apache.fineract.test.data.ChargeCalculationType;
import org.apache.fineract.test.data.ChargeProductAppliesTo;
import org.apache.fineract.test.data.ChargeTimeType;
import org.apache.fineract.test.factory.WorkingCapitalChargeRequestFactory;
import org.apache.fineract.test.stepdef.AbstractStepDef;
import org.apache.fineract.test.support.TestContextKey;

@Slf4j
@RequiredArgsConstructor
public class WorkingCapitalChargeStepDef extends AbstractStepDef {

    private static final Long REGULAR_PAYMENT_MODE_ID = 0L;
    private static final Long SPECIFIED_DUE_DATE_ID = 2L;
    private static final Long FLAT_CALCULATION_TYPE_ID = 1L;

    private final FineractFeignClient fineractClient;
    private final WorkingCapitalChargeRequestFactory chargeRequestFactory;

    @When("Admin creates working capital loan charge")
    public void createWorkingCapitalLoanCharge() {
        createChargeAndStore(chargeRequestFactory.defaultWorkingCapitalChargeRequest());
    }

    @When("Admin creates working capital loan charge as penalty")
    public void createWorkingCapitalLoanChargeAsPenalty() {
        createChargeAndStore(chargeRequestFactory.defaultWorkingCapitalChargeRequest().penalty(true).amount(15.0D));
    }

    @When("Admin creates working capital loan charge without payment mode")
    public void createWorkingCapitalLoanChargeWithoutPaymentMode() {
        createChargeAndStore(chargeRequestFactory.defaultWorkingCapitalChargeRequest().amount(25.0D).chargePaymentMode(null));
    }

    @When("Admin creates working capital loan charge with {string} charge time type and {string} calculation type")
    public void createWorkingCapitalLoanChargeWithParams(String chargeTimeTypeName, String chargeCalcTypeName) {
        final ChargeTimeType timeType = ChargeTimeType.valueOf(chargeTimeTypeName);
        final ChargeCalculationType calcType = ChargeCalculationType.valueOf(chargeCalcTypeName);
        createChargeAndStore(chargeRequestFactory.defaultWorkingCapitalChargeRequest() //
                .chargeTimeType(timeType.value) //
                .chargeCalculationType(calcType.value));
    }

    @When("Admin updates working capital loan charge")
    public void updateWorkingCapitalLoanCharge() {
        final Long id = getChargeId();
        final ChargeRequest request = chargeRequestFactory.defaultWorkingCapitalChargeRequest().amount(30.0D).penalty(true);
        ok(() -> fineractClient.charges().updateCharge(id, request));
    }

    @When("Admin deletes working capital loan charge")
    public void deleteWorkingCapitalLoanCharge() {
        ok(() -> fineractClient.charges().deleteCharge(getChargeId()));
    }

    @Then("Admin retrieves working capital loan charge and verifies it is a penalty")
    public void retrieveAndVerifyPenalty() {
        final Long id = getChargeId();
        final GetChargesResponse chargeData = retrieveCharge(id);
        assertThat(chargeData.getPenalty()).as("Charge should be a penalty").isTrue();
        assertThat(chargeData.getActive()).as("Charge should be active").isTrue();
        log.info("Verified WCL charge ID {} is a penalty", id);
    }

    @Then("Admin retrieves working capital loan charge and verifies payment mode is Regular")
    public void retrieveAndVerifyPaymentModeRegular() {
        final Long id = getChargeId();
        final GetChargesResponse chargeData = retrieveCharge(id);
        assertThat(chargeData.getChargePaymentMode()).as("Charge payment mode should not be null").isNotNull();
        assertThat(chargeData.getChargePaymentMode().getId()).as("Payment mode should be Regular (0)").isEqualTo(REGULAR_PAYMENT_MODE_ID);
        log.info("Verified WCL charge ID {} has Regular payment mode", id);
    }

    @Then("Admin retrieves the charge template for Working Capital Loan")
    public void retrieveChargeTemplateForWcl() {
        final ChargeData templateData = ok(() -> fineractClient.charges()
                .retrieveTemplateCharge(Map.of("chargeAppliesTo", ChargeProductAppliesTo.WORKING_CAPITAL_LOAN.value)));
        testContext().set(TestContextKey.WORKING_CAPITAL_CHARGE_TEMPLATE, templateData);
        log.info("Retrieved charge template for Working Capital Loan");
    }

    @Then("Admin retrieves the charge template for Working Capital Loan with charge time type {string}")
    public void retrieveChargeTemplateForWclWithTimeType(String chargeTimeTypeName) {
        final ChargeTimeType timeType = ChargeTimeType.valueOf(chargeTimeTypeName);
        final ChargeData templateData = ok(() -> fineractClient.charges().retrieveTemplateCharge(
                Map.of("chargeAppliesTo", ChargeProductAppliesTo.WORKING_CAPITAL_LOAN.value, "chargeTimeType", timeType.value)));
        testContext().set(TestContextKey.WORKING_CAPITAL_CHARGE_TEMPLATE, templateData);
        log.info("Retrieved charge template for Working Capital Loan with chargeTimeType={}", chargeTimeTypeName);
    }

    @Then("The charge template chargeTimeTypeOptions contains only Specified due date")
    public void verifyTemplateChargeTimeTypeOptions() {
        assertSingleOption(getChargeTemplate().getChargeTimeTypeOptions(), "chargeTimeTypeOptions", SPECIFIED_DUE_DATE_ID);
        log.info("Verified charge template chargeTimeTypeOptions contains only Specified due date");
    }

    @Then("The charge template chargeCalculationTypeOptions contains only Flat")
    public void verifyTemplateChargeCalculationTypeOptions() {
        assertSingleOption(getChargeTemplate().getChargeCalculationTypeOptions(), "chargeCalculationTypeOptions", FLAT_CALCULATION_TYPE_ID);
        log.info("Verified charge template chargeCalculationTypeOptions contains only Flat");
    }

    @Then("The charge template chargePaymentModeOptions contains only Regular")
    public void verifyTemplateChargePaymentModeOptions() {
        assertSingleOption(getChargeTemplate().getChargePaymetModeOptions(), "chargePaymentModeOptions", REGULAR_PAYMENT_MODE_ID);
        log.info("Verified charge template chargePaymentModeOptions contains only Regular");
    }

    @Then("Creating working capital loan charge with {string} chargeTimeType and {string} chargeCalculationType results an error with the following data:")
    public void createWclChargeWithInvalidParamsFails(String chargeTimeTypeName, String chargeCalcTypeName, DataTable table) {
        final ChargeTimeType timeType = ChargeTimeType.valueOf(chargeTimeTypeName);
        final ChargeCalculationType calcType = ChargeCalculationType.valueOf(chargeCalcTypeName);
        final ChargeRequest request = chargeRequestFactory.defaultWorkingCapitalChargeRequest() //
                .chargeTimeType(timeType.value) //
                .chargeCalculationType(calcType.value);

        final Map<String, String> expectedData = table.asMaps().get(0);
        final int expectedHttpCode = Integer.parseInt(expectedData.get("httpCode"));
        final String expectedErrorMessage = expectedData.get("errorMessage").trim();

        final CallFailedRuntimeException exception = fail(() -> fineractClient.charges().createCharge(request));
        assertHttpStatus(exception, expectedHttpCode);
        assertErrorMessage(exception, expectedErrorMessage);
        log.info("Verified creating WCL charge with chargeTimeType={} and calcType={} failed with status {} and message: {}",
                chargeTimeTypeName, chargeCalcTypeName, exception.getStatus(), expectedErrorMessage);
    }

    // Charge API Helpers
    private void createChargeAndStore(final ChargeRequest request) {
        final PostChargesResponse response = ok(() -> fineractClient.charges().createCharge(request));
        testContext().set(TestContextKey.WORKING_CAPITAL_CHARGE_ID, response.getResourceId());
        log.info("Created WCL charge with ID: {}", response.getResourceId());
    }

    private GetChargesResponse retrieveCharge(final Long chargeId) {
        final GetChargesResponse chargeData = ok(() -> fineractClient.charges().retrieveOneCharge(chargeId));
        assertThat(chargeData).as("Charge data should not be null").isNotNull();
        return chargeData;
    }

    // Data Extraction Helpers
    private Long getChargeId() {
        return testContext().get(TestContextKey.WORKING_CAPITAL_CHARGE_ID);
    }

    private ChargeData getChargeTemplate() {
        final ChargeData templateData = testContext().get(TestContextKey.WORKING_CAPITAL_CHARGE_TEMPLATE);
        assertThat(templateData).as("Charge template should not be null").isNotNull();
        return templateData;
    }

    // Assertion Helpers
    private void assertHttpStatus(final CallFailedRuntimeException exception, final int expectedStatus) {
        assertThat(exception.getStatus()).as("HTTP status code should be " + expectedStatus).isEqualTo(expectedStatus);
    }

    private void assertErrorMessage(final CallFailedRuntimeException exception, final String expectedMessage) {
        assertThat(exception.getMessage()).as("Error message should contain: " + expectedMessage).contains(expectedMessage);
    }

    private void assertSingleOption(final List<EnumOptionData> options, final String optionName, final Long expectedId) {
        assertThat(options).as(optionName + " should not be null or empty").isNotNull().isNotEmpty();
        assertThat(options).hasSize(1);
        assertThat(options.get(0).getId()).as("Only " + optionName + " with ID " + expectedId + " should be available")
                .isEqualTo(expectedId);
    }
}
