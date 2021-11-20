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

import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.config.BatchDestinations;
import org.apache.fineract.infrastructure.core.utils.ProfileUtils;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public class BatchWriterBase {

    @Autowired
    protected ApplicationContext context;

    protected StepExecution stepExecution;
    protected JobParameters parameters;
    protected ProfileUtils profileUtils;
    protected BatchDestinations batchDestinations;

    protected Gson gson;

    protected String tenantIdentifier;
    protected String batchStepName;
    protected String queueName;

    protected int processed;

    protected void initialize(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.batchStepName = stepExecution.getStepName();
        this.parameters = this.stepExecution.getJobExecution().getJobParameters();
        this.tenantIdentifier = this.parameters.getString(BatchConstants.JOB_PARAM_TENANT_ID);

        this.profileUtils = new ProfileUtils(context.getEnvironment());
        this.gson = new Gson();

        this.processed = 0;
    }
}
