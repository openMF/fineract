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

import com.google.gson.Gson;
import java.util.Date;
import org.apache.fineract.infrastructure.batch.config.BatchConstants;
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

    protected final static ThreadLocal<String> batchStepName = new ThreadLocal<>();
    protected final static ThreadLocal<String> tenantIdentifier = new ThreadLocal<>();

    protected final static ThreadLocal<JobParameters> parameters = new ThreadLocal<>();

    protected final static ThreadLocal<Integer> processed = new ThreadLocal<>();
    protected final static ThreadLocal<String> dateOfTenantValue = new ThreadLocal<>();
    protected final static ThreadLocal<String> cobDateValue = new ThreadLocal<>();
    protected final static ThreadLocal<Date> dateOfTenant = new ThreadLocal<>();
    protected AppUser appUser;

    protected final Gson gson = new Gson();

    protected void initialize(StepExecution stepExecution) {
        batchStepName.set(stepExecution.getStepName());
        parameters.set(stepExecution.getJobExecution().getJobParameters());
        tenantIdentifier.set(parameters.get().getString(BatchConstants.JOB_PARAM_TENANT_ID));

        dateOfTenantValue.set(parameters.get().getString(BatchConstants.JOB_PARAM_TENANT_DATE));
        dateOfTenant.set(DateUtils.createDate(dateOfTenantValue.get(), BatchConstants.DEFAULT_BATCH_DATE_FORMAT));
        cobDateValue.set(parameters.get().getString(BatchConstants.JOB_PARAM_COB_DATE));

        this.appUser = this.appUserRepository.fetchSystemUser();
        processed.set(0);
    }

    protected void cleanup() {
        batchStepName.remove();
        tenantIdentifier.remove();
        parameters.remove();
        processed.remove();
        dateOfTenantValue.remove();
        cobDateValue.remove();
        dateOfTenant.remove();
    }
}
