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

import java.util.List;

import com.google.gson.Gson;

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

    protected StepExecution stepExecution;
    protected JobParameters parameters;
    protected ProfileUtils profileUtils;
    protected BatchDestinations batchDestinations;

    protected Gson gson;

    protected String tenantIdentifier;
    protected Long batchJobInstanceId;
    protected String batchStepName;
    protected String queueName;

    protected int processed;
    protected int chunkCounter;

    protected void initialize(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.batchStepName = stepExecution.getStepName();
        this.batchJobInstanceId = stepExecution.getJobExecution().getJobInstance().getInstanceId();
        this.parameters = this.stepExecution.getJobExecution().getJobParameters();
        this.tenantIdentifier = this.parameters.getString(BatchConstants.JOB_PARAM_TENANT_ID);
        BatchJobUtils.setTenant(tenantIdentifier, tenantDetailsService);

        this.profileUtils = new ProfileUtils(context.getEnvironment());
        this.gson = new Gson();

        this.processed = 0;
        this.chunkCounter = 0;
    }

    protected MessageBatchDataResults analyzeResults(List<? extends MessageBatchDataResponse> items) {
        final int totalItems = items.size();
        for (final MessageBatchDataResponse processResult : items) {
            if (processResult.wasChanged())
                this.processed++;
        }
        return new MessageBatchDataResults(this.tenantIdentifier, this.batchStepName, totalItems, this.processed, (totalItems - this.processed), true);
    }

    protected void sendMessage(final MessageBatchDataResults message) {
        // final String payload = gson.toJson(message);
        // LOG.debug("Sending notification {}: {}", this.batchStepName, payload);
        this.businessEventNotifierService.notifyBusinessEventWasExecuted(BusinessEvents.COB_STEP_EXECUTION,
            ListUtils.constructEntityMap(BusinessEntity.BATCH_JOB, message));
    }
}
