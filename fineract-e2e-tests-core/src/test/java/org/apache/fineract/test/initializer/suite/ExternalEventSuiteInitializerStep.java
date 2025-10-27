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
package org.apache.fineract.test.initializer.suite;

import static org.apache.fineract.client.feign.util.FeignCalls.executeVoid;
import static org.apache.fineract.client.feign.util.FeignCalls.ok;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.models.ExternalEventConfigurationItemResponse;
import org.apache.fineract.client.models.ExternalEventConfigurationResponse;
import org.apache.fineract.client.models.ExternalEventConfigurationUpdateRequest;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class ExternalEventSuiteInitializerStep implements FineractSuiteInitializerStep {

    private final FineractFeignClient fineractClient;

    @Override
    public void initializeForSuite() {
        Map<String, Boolean> eventConfigMap = new HashMap<>();

        ExternalEventConfigurationResponse response = ok(
                () -> fineractClient.externalEventConfiguration().getExternalEventConfigurations());

        List<ExternalEventConfigurationItemResponse> externalEventConfiguration = response.getExternalEventConfiguration();
        externalEventConfiguration.forEach(e -> {
            eventConfigMap.put(e.getType(), true);
        });

        ExternalEventConfigurationUpdateRequest request = new ExternalEventConfigurationUpdateRequest()
                .externalEventConfigurations(eventConfigMap);

        executeVoid(() -> fineractClient.externalEventConfiguration().updateExternalEventConfigurations(request, Collections.emptyMap()));
    }
}
