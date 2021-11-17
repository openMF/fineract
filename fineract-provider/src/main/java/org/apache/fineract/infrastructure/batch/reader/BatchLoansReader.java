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
package org.apache.fineract.infrastructure.batch.reader;

import java.util.Collection;
import java.util.Iterator;
import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.security.service.TenantDetailsService;
import org.apache.fineract.portfolio.loanaccount.service.LoanReadPlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;

public class BatchLoansReader implements ItemReader<Long> {

    public static final Logger LOG = LoggerFactory.getLogger(BatchLoansReader.class);

    @Autowired
    private LoanReadPlatformService loanReadPlatformService;

    @Autowired
    private TenantDetailsService tenantDetailsService;

    private Iterator<Long> dataIterator;
    private StepExecution stepExecution;

    @BeforeStep
    public void before(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        JobParameters parameters = stepExecution.getJobExecution().getJobParameters();
        final String tenantIdentifier = parameters.getString(BatchConstants.JOB_PARAM_TENANT_ID);
        final FineractPlatformTenant tenant = this.tenantDetailsService.loadTenantById(tenantIdentifier);
        ThreadLocalContextUtil.setTenant(tenant);
        LOG.debug("Tenant {}", tenant.getName());

        final Collection<Long> loansForBatchProcess = this.loanReadPlatformService.fetchLoansForBatchProcess();
        this.dataIterator = loansForBatchProcess.iterator();
    }

    @Override
    public Long read() {
        if (dataIterator != null && dataIterator.hasNext()) {
            return dataIterator.next();
        } else {
            return null;
        }
    }
}
