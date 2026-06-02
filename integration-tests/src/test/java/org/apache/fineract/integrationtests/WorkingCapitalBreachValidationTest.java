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
package org.apache.fineract.integrationtests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.fineract.client.feign.util.CallFailedRuntimeException;
import org.apache.fineract.client.models.WorkingCapitalBreachRequest;
import org.apache.fineract.integrationtests.common.Utils;
import org.apache.fineract.integrationtests.common.workingcapitalloanbreach.WorkingCapitalBreachHelper;
import org.junit.jupiter.api.Test;

public class WorkingCapitalBreachValidationTest {

    private final WorkingCapitalBreachHelper breachHelper = new WorkingCapitalBreachHelper();

    @Test
    public void testCreateFailsWhenBreachFrequencyIsMissing() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachFrequency(null);
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachFrequency"));
    }

    @Test
    public void testCreateFailsWhenNameIsMissing() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.name(null);
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("name"));
    }

    @Test
    public void testCreateFailsWhenNameIsBlank() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.name("     ");
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("name"));
    }

    @Test
    public void testCreateFailsWhenNameTooLong() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.name("x".repeat(101));
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("name"));
    }

    @Test
    public void testCreateFailsWhenBreachFrequencyTypeIsMissing() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachFrequencyType(null);
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachFrequencyType"));
    }

    @Test
    public void testCreateFailsWhenBreachAmountCalculationTypeIsMissing() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachAmountCalculationType(null);
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachAmountCalculationType"));
    }

    @Test
    public void testCreateFailsWhenBreachAmountIsMissing() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachAmount(null);
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachAmount"));
    }

    @Test
    public void testCreateFailsWhenBreachFrequencyIsZero() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachFrequency(0);
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachFrequency"));
    }

    @Test
    public void testCreateFailsWhenBreachFrequencyTypeIsInvalid() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachFrequencyType("HOUR");
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachFrequencyType"));
    }

    @Test
    public void testCreateFailsWhenBreachAmountCalculationTypeIsInvalid() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachAmountCalculationType("UNKNOWN");
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachAmountCalculationType"));
    }

    @Test
    public void testCreateFailsWhenBreachAmountIsNegative() {
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachAmount(BigDecimal.valueOf(-1));
        final CallFailedRuntimeException ex = breachHelper.runCreateExpectingFailure(request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachAmount"));
    }

    @Test
    public void testUpdateFailsWhenBreachFrequencyTypeIsInvalid() {
        final WorkingCapitalBreachRequest createBody = breachHelper.createBreachRequest(Utils.randomStringGenerator("Breach", 20), 20,
                "DAYS", "PERCENTAGE", BigDecimal.valueOf(7.5));
        final Long breachId = breachHelper.create(createBody);
        final WorkingCapitalBreachRequest request = validBreachRequest();
        request.breachFrequencyType("INVALID");

        final CallFailedRuntimeException ex = breachHelper.runUpdateExpectingFailure(breachId, request);
        assertEquals(400, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
        assertTrue(ex.getDeveloperMessage().contains("breachFrequencyType"));

        breachHelper.delete(breachId);
    }

    @Test
    public void testRetrieveOneFailsWhenBreachNotFound() {
        final Long nonExistingId = 9_999_999_999L;
        final CallFailedRuntimeException ex = breachHelper.runRetrieveOneExpectingFailure(nonExistingId);
        assertEquals(404, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
    }

    @Test
    public void testUpdateFailsWhenBreachNotFound() {
        final Long nonExistingId = 9_999_999_998L;
        final CallFailedRuntimeException ex = breachHelper.runUpdateExpectingFailure(nonExistingId, validBreachRequest());
        assertEquals(404, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
    }

    @Test
    public void testDeleteFailsWhenBreachNotFound() {
        final Long nonExistingId = 9_999_999_997L;
        final CallFailedRuntimeException ex = breachHelper.runDeleteExpectingFailure(nonExistingId);
        assertEquals(404, ex.getStatus());
        assertNotNull(ex.getDeveloperMessage());
    }

    private static WorkingCapitalBreachRequest validBreachRequest() {
        return new WorkingCapitalBreachRequest() //
                .name("Default WCL Breach") //
                .breachFrequency(30) //
                .breachFrequencyType("DAYS") //
                .breachAmountCalculationType("PERCENTAGE") //
                .breachAmount(BigDecimal.TEN); //
    }
}
