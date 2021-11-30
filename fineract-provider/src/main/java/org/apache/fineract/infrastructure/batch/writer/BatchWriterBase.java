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
package org.apache.fineract.infrastructure.batch.writer;

import com.google.gson.Gson;
import java.util.List;
import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.config.BatchDestinations;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResponse;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResults;
import org.apache.fineract.infrastructure.batch.service.BatchJobUtils;
import org.apache.fineract.infrastructure.core.utils.ListUtils;
import org.apache.fineract.infrastructure.core.utils.ProfileUtils;
import org.apache.fineract.infrastructure.security.service.TenantDetailsService;
import org.apache.fineract.portfolio.common.BusinessEventNotificationConstants.BusinessEntity;
import org.apache.fineract.portfolio.common.BusinessEventNotificationConstants.BusinessEvents;
import org.apache.fineract.portfolio.common.service.BusinessEventNotifierService;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public class BatchWriterBase {

    @Autowired
    protected TenantDetailsService tenantDetailsService;

    @Autowired
    protected ApplicationContext context;

    @Autowired
    protected BusinessEventNotifierService businessEventNotifierService;

    protected final static ThreadLocal<JobParameters> parameters = new ThreadLocal<>();
    protected ProfileUtils profileUtils;
    protected BatchDestinations batchDestinations;

    protected Gson gson;

    protected final static ThreadLocal<String> tenantIdentifier = new ThreadLocal<>();
    protected final static ThreadLocal<Long> batchJobInstanceId = new ThreadLocal<>();
    protected final static ThreadLocal<String> batchStepName = new ThreadLocal<>();
    protected String queueName;

    protected final static ThreadLocal<Integer> processed = new ThreadLocal<>();
    protected final static ThreadLocal<Integer> chunkCounter = new ThreadLocal<>();

    protected void initialize(StepExecution stepExecution) {
        batchStepName.set(stepExecution.getStepName());
        batchJobInstanceId.set(stepExecution.getJobExecution().getJobInstance().getInstanceId());
        parameters.set(stepExecution.getJobExecution().getJobParameters());
        tenantIdentifier.set(parameters.get().getString(BatchConstants.JOB_PARAM_TENANT_ID));
        BatchJobUtils.setTenant(tenantIdentifier.get(), tenantDetailsService);

        this.profileUtils = new ProfileUtils(context.getEnvironment());
        this.gson = new Gson();

        processed.set(0);
        chunkCounter.set(0);
    }

    protected void cleanup() {
        tenantIdentifier.remove();
        batchJobInstanceId.remove();
        batchStepName.remove();
        parameters.remove();
        processed.remove();
        chunkCounter.remove();
    }

    protected MessageBatchDataResults analyzeResults(List<? extends MessageBatchDataResponse> items) {
        final int totalItems = items.size();
        for (final MessageBatchDataResponse processResult : items) {
            if (processResult.wasChanged())
                processed.set(processed.get() + 1);
        }
        return new MessageBatchDataResults(tenantIdentifier.get(), batchStepName.get(), totalItems, processed.get(),
                (totalItems - processed.get()), true);
    }

    protected void sendMessage(final MessageBatchDataResults message) {
        // final String payload = gson.toJson(message);
        // LOG.debug("Sending notification {}: {}", batchStepName.get(), payload);
        this.businessEventNotifierService.notifyBusinessEventWasExecuted(BusinessEvents.COB_STEP_EXECUTION,
                ListUtils.constructEntityMap(BusinessEntity.BATCH_JOB, message));
    }
}
