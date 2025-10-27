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

import lombok.RequiredArgsConstructor;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.models.GetJobsResponse;
import org.apache.fineract.client.models.PutJobsJobIDRequest;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JobSuiteInitializerStep implements FineractSuiteInitializerStep {

    public static final String SEND_ASYNCHRONOUS_EVENTS_JOB_NAME = "Send Asynchronous Events";
    public static final String EVERY_1_SECONDS = "0/1 * * * * ?";
    public static final String EVERY_60_SECONDS = "0 0/1 * * * ?";

    private final FineractFeignClient fineractClient;

    @Override
    public void initializeForSuite() {
        updateExternalEventJobFrequency(EVERY_1_SECONDS);
    }

    @Override
    public void resetAfterSuite() {
        updateExternalEventJobFrequency(EVERY_60_SECONDS);
    }

    private void updateExternalEventJobFrequency(String cronExpression) {
        GetJobsResponse externalEventJobResponse = ok(() -> fineractClient.schedulerJob().retrieveAll8()).stream()
                .filter(r -> r.getDisplayName().equals(SEND_ASYNCHRONOUS_EVENTS_JOB_NAME)).findAny()
                .orElseThrow(() -> new IllegalStateException(SEND_ASYNCHRONOUS_EVENTS_JOB_NAME + " is not found"));
        Long jobId = externalEventJobResponse.getJobId();
        executeVoid(() -> fineractClient.schedulerJob().updateJobDetail(jobId, new PutJobsJobIDRequest().cronExpression(cronExpression)));
    }
}
