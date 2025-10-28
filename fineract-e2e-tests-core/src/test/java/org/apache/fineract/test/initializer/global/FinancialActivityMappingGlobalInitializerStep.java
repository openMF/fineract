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
package org.apache.fineract.test.initializer.global;

import static org.apache.fineract.client.feign.util.FeignCalls.executeVoid;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.models.GetFinancialActivityAccountsResponse;
import org.apache.fineract.client.models.PostFinancialActivityAccountsRequest;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class FinancialActivityMappingGlobalInitializerStep implements FineractGlobalInitializerStep {

    public static final Long FINANCIAL_ACTIVITY_ID_ASSET_TRANSFER = 100L;
    public static final Long GL_ACCOUNT_ID_ASSET_TRANSFER = 21L;

    private final FineractFeignClient fineractClient;

    @Override
    public void initialize() {
        List<GetFinancialActivityAccountsResponse> existingMappings = new ArrayList<>();
        try {
            existingMappings = fineractClient.mappingFinancialActivitiesToAccounts().retrieveAll();
        } catch (Exception e) {
            log.debug("Could not retrieve existing financial activity mappings, will create them", e);
        }

        boolean mappingExists = existingMappings.stream()
                .anyMatch(m -> FINANCIAL_ACTIVITY_ID_ASSET_TRANSFER.equals(m.getFinancialActivityData().getId())
                        && GL_ACCOUNT_ID_ASSET_TRANSFER.equals(m.getGlAccountData().getId()));

        if (mappingExists) {
            return;
        }

        PostFinancialActivityAccountsRequest request = new PostFinancialActivityAccountsRequest()
                .financialActivityId(FINANCIAL_ACTIVITY_ID_ASSET_TRANSFER).glAccountId(GL_ACCOUNT_ID_ASSET_TRANSFER);
        executeVoid(() -> fineractClient.mappingFinancialActivitiesToAccounts().createGLAccount(request));
    }
}
