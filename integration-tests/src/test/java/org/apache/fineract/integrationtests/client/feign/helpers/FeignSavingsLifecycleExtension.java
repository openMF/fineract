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
package org.apache.fineract.integrationtests.client.feign.helpers;

import java.util.List;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.integrationtests.client.feign.modules.SavingsTestData;
import org.apache.fineract.integrationtests.common.FineractFeignClientHelper;
import org.apache.fineract.integrationtests.common.Utils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeignSavingsLifecycleExtension implements AfterEachCallback, BeforeEachCallback {

    private static final Logger LOG = LoggerFactory.getLogger(FeignSavingsLifecycleExtension.class);

    @Override
    public void beforeEach(ExtensionContext context) {
        cleanupSavings();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        cleanupSavings();
    }

    private void cleanupSavings() {
        try {
            FineractFeignClient client = FineractFeignClientHelper.getFineractFeignClient();
            FeignSavingsHelper savingsHelper = new FeignSavingsHelper(client);
            String todayDate = Utils.dateFormatter.format(Utils.getLocalDateOfTenant());

            closeActiveAccounts(client, savingsHelper, todayDate);
            undoApprovedAccounts(client, savingsHelper);
            rejectSubmittedAccounts(client, savingsHelper, todayDate);
        } catch (Exception e) {
            LOG.warn("Cleanup: savings cleanup failed: {}", e.getMessage());
        }
    }

    private void closeActiveAccounts(FineractFeignClient client, FeignSavingsHelper savingsHelper, String todayDate) {
        try {
            List<Long> activeIds = client.defaultApi().getSavingsAccountsByStatus(SavingsTestData.SavingsStatus.ACTIVE);
            for (Long id : activeIds) {
                try {
                    savingsHelper.closeSavings(id, todayDate, true);
                } catch (Exception e) {
                    LOG.warn("Cleanup: could not close savings {}: {}", id, e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("Cleanup: could not list active savings: {}", e.getMessage());
        }
    }

    private void undoApprovedAccounts(FineractFeignClient client, FeignSavingsHelper savingsHelper) {
        try {
            List<Long> approvedIds = client.defaultApi().getSavingsAccountsByStatus(SavingsTestData.SavingsStatus.APPROVED);
            for (Long id : approvedIds) {
                try {
                    savingsHelper.undoApproval(id);
                } catch (Exception e) {
                    LOG.warn("Cleanup: could not undo approval for savings {}: {}", id, e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("Cleanup: could not list approved savings: {}", e.getMessage());
        }
    }

    private void rejectSubmittedAccounts(FineractFeignClient client, FeignSavingsHelper savingsHelper, String todayDate) {
        try {
            List<Long> submittedIds = client.defaultApi().getSavingsAccountsByStatus(SavingsTestData.SavingsStatus.SUBMITTED);
            for (Long id : submittedIds) {
                try {
                    savingsHelper.rejectSavings(id, todayDate);
                } catch (Exception e) {
                    LOG.warn("Cleanup: could not reject savings {}: {}", id, e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("Cleanup: could not list submitted savings: {}", e.getMessage());
        }
    }
}
