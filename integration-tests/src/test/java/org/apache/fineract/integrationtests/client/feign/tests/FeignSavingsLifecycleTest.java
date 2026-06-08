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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.fineract.client.feign.util.CallFailedRuntimeException;
import org.apache.fineract.client.models.SavingsAccountData;
import org.apache.fineract.integrationtests.client.feign.FeignSavingsTestBase;
import org.apache.fineract.integrationtests.common.Utils;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FeignSavingsLifecycleTest extends FeignSavingsTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(FeignSavingsLifecycleTest.class);

    @Test
    @Order(1)
    void testCreateSavingsProduct() {
        Long productId = createDefaultSavingsProduct().getResourceId();
        assertNotNull(productId, "Savings product ID should not be null");
        LOG.info("Created savings product with ID: {}", productId);

        var product = getSavingsProduct(productId);
        assertNotNull(product.getName(), "Savings product name should not be null");
    }

    @Test
    @Order(2)
    void testSubmitSavingsApplication() {
        Long clientId = createClient();
        Long productId = createDefaultSavingsProduct().getResourceId();
        String today = Utils.dateFormatter.format(Utils.getLocalDateOfTenant());

        Long savingsId = submitSavingsApplication(clientId, productId, today).getSavingsId();
        assertNotNull(savingsId, "Savings account ID should not be null");

        verifySavingsStatus(savingsId, status -> status.getSubmittedAndPendingApproval());
        LOG.info("Submitted savings application with ID: {}", savingsId);
    }

    @Test
    @Order(3)
    void testApproveSavingsApplication() {
        Long clientId = createClient();
        Long productId = createDefaultSavingsProduct().getResourceId();
        String today = Utils.dateFormatter.format(Utils.getLocalDateOfTenant());

        Long savingsId = submitSavingsApplication(clientId, productId, today).getSavingsId();
        approveSavings(savingsId, today);

        verifySavingsStatus(savingsId, status -> status.getApproved());
        LOG.info("Approved savings application with ID: {}", savingsId);
    }

    @Test
    @Order(4)
    void testActivateSavingsAccount() {
        Long clientId = createClient();
        Long productId = createDefaultSavingsProduct().getResourceId();
        String today = Utils.dateFormatter.format(Utils.getLocalDateOfTenant());

        Long savingsId = submitSavingsApplication(clientId, productId, today).getSavingsId();
        approveSavings(savingsId, today);
        activateSavings(savingsId, today);

        verifySavingsStatus(savingsId, status -> status.getActive());
        LOG.info("Activated savings account with ID: {}", savingsId);
    }

    @Test
    @Order(5)
    void testDepositAndWithdraw() {
        Long clientId = createClient();
        Long productId = createDefaultSavingsProduct().getResourceId();
        String today = Utils.dateFormatter.format(Utils.getLocalDateOfTenant());

        Long savingsId = createApproveActivateSavings(clientId, productId, today);

        deposit(savingsId, "5000", today);
        withdraw(savingsId, "2000", today);

        SavingsAccountData details = getSavingsDetails(savingsId);
        BigDecimal balance = details.getSummary().getAvailableBalance();
        assertNotNull(balance, "Available balance should not be null");
        assertEquals(0, new BigDecimal("3000").compareTo(balance),
                "Expected balance of 3000 after depositing 5000 and withdrawing 2000, but got " + balance);
        LOG.info("Deposit/withdraw test passed. Account {} balance: {}", savingsId, balance);
    }

    @Test
    @Order(6)
    void testCloseSavingsAccount() {
        Long clientId = createClient();
        Long productId = createDefaultSavingsProduct().getResourceId();
        String today = Utils.dateFormatter.format(Utils.getLocalDateOfTenant());

        Long savingsId = createApproveActivateSavings(clientId, productId, today);

        closeSavings(savingsId, today, false);

        verifySavingsStatus(savingsId, status -> status.getClosed());
        LOG.info("Closed savings account with ID: {}", savingsId);
    }

    @Test
    @Order(7)
    void testDeletePendingSavingsApplication() {
        Long clientId = createClient();
        Long productId = createDefaultSavingsProduct().getResourceId();
        String today = Utils.dateFormatter.format(Utils.getLocalDateOfTenant());

        Long savingsId = submitSavingsApplication(clientId, productId, today).getSavingsId();
        assertNotNull(savingsId);

        deleteSavingsApplication(savingsId);

        assertThrows(CallFailedRuntimeException.class, () -> getSavingsDetails(savingsId),
                "Retrieving a deleted savings account should fail");
        LOG.info("Deleted pending savings application with ID: {}", savingsId);
    }

    @Test
    @Order(8)
    void testFullSavingsLifecycle() {

        Long clientId = createClient();
        Long productId = createDefaultSavingsProduct().getResourceId();
        String today = Utils.dateFormatter.format(Utils.getLocalDateOfTenant());

        Long savingsId = submitSavingsApplication(clientId, productId, today).getSavingsId();
        verifySavingsStatus(savingsId, status -> status.getSubmittedAndPendingApproval());

        approveSavings(savingsId, today);
        verifySavingsStatus(savingsId, status -> status.getApproved());

        activateSavings(savingsId, today);
        verifySavingsStatus(savingsId, status -> status.getActive());

        deposit(savingsId, "10000", today);
        SavingsAccountData afterDeposit = getSavingsDetails(savingsId);
        assertTrue(new BigDecimal("10000").compareTo(afterDeposit.getSummary().getAvailableBalance()) == 0,
                "Balance after deposit should be 10000");

        withdraw(savingsId, "3500", today);
        SavingsAccountData afterWithdraw = getSavingsDetails(savingsId);
        assertTrue(new BigDecimal("6500").compareTo(afterWithdraw.getSummary().getAvailableBalance()) == 0,
                "Balance after withdrawal should be 6500");

        closeSavings(savingsId, today, true);
        verifySavingsStatus(savingsId, status -> status.getClosed());

        LOG.info("Full savings lifecycle test passed for account {}", savingsId);
    }
}
