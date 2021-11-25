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
package org.apache.fineract.infrastructure.batch.processor;

import java.util.Date;

import com.google.gson.Gson;

import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.service.BatchJobUtils;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.apache.fineract.infrastructure.security.service.TenantDetailsService;
import org.apache.fineract.useradministration.domain.AppUser;
import org.apache.fineract.useradministration.domain.AppUserRepositoryWrapper;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;

public class BatchProcessorBase {
    
    @Autowired
    protected TenantDetailsService tenantDetailsService;

    @Autowired
    protected PlatformSecurityContext context;

    @Autowired 
    protected AppUserRepositoryWrapper appUserRepository;

    protected String batchStepName;
    protected String tenantIdentifier;

    protected StepExecution stepExecution;
    protected JobParameters parameters;

    protected int processed;
    protected String dateOfTenantValue;
    protected String cobDateValue;
    protected Date dateOfTenant;
    protected FineractPlatformTenant tenant;
    protected AppUser appUser;

    protected final Gson gson = new Gson();

    protected void initialize(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.batchStepName = stepExecution.getStepName();
        this.parameters = this.stepExecution.getJobExecution().getJobParameters();
        this.tenantIdentifier = parameters.getString(BatchConstants.JOB_PARAM_TENANT_ID);
        this.tenant = BatchJobUtils.setTenant(tenantIdentifier, tenantDetailsService);

        this.dateOfTenantValue = parameters.getString(BatchConstants.JOB_PARAM_TENANT_DATE);
        this.dateOfTenant = DateUtils.createDate(this.dateOfTenantValue, BatchConstants.DEFAULT_BATCH_DATE_FORMAT);
        this.cobDateValue = parameters.getString(BatchConstants.JOB_PARAM_COB_DATE);
        
        this.appUser = this.appUserRepository.fetchSystemUser();
        this.processed = 0;
    }
}
