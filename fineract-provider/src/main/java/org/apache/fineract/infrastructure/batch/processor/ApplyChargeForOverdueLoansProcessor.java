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

import java.util.Collection;

import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResponse;
import org.apache.fineract.infrastructure.batch.service.BatchJobUtils;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.utils.TextUtils;
import org.apache.fineract.infrastructure.security.service.TenantDetailsService;
import org.apache.fineract.portfolio.loanaccount.loanschedule.data.OverdueLoanScheduleData;
import org.apache.fineract.portfolio.loanaccount.service.LoanReadPlatformService;
import org.apache.fineract.portfolio.loanaccount.service.LoanWritePlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.JobParameters;

public class ApplyChargeForOverdueLoansProcessor implements ItemProcessor<Long, MessageBatchDataResponse> {

    public static final Logger LOG = LoggerFactory.getLogger(ApplyChargeForOverdueLoansProcessor.class);

    @Autowired
    private LoanReadPlatformService loanReadPlatformService;

    @Autowired
    private LoanWritePlatformService loanWritePlatformService;

    @Autowired
    private TenantDetailsService tenantDetailsService;

    private String batchJobName;
    private String tenantIdentifier;
    private Long penaltyWaitPeriodValue;
    private Boolean backdatePenalties;

    private StepExecution stepExecution;

    @BeforeStep
    public void before(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.batchJobName = stepExecution.getJobExecution().getJobInstance().getJobName();
        JobParameters parameters = this.stepExecution.getJobExecution().getJobParameters();
        this.tenantIdentifier = parameters.getString(BatchConstants.JOB_PARAM_TENANT_ID);
        final FineractPlatformTenant tenant = BatchJobUtils.setTenant(tenantIdentifier, tenantDetailsService);
        LOG.debug("Tenant {}", tenant.getName());

        this.penaltyWaitPeriodValue = parameters.getLong(BatchConstants.JOB_PARAM_PENALTY_WAIT_PERIOD);
        this.backdatePenalties = TextUtils.stringToBoolean(parameters.getString(BatchConstants.JOB_PARAM_BACKDATE_PENALTIES));
    }

    @Override
    public MessageBatchDataResponse process(Long entityId) throws Exception {
        LOG.debug("processing: " + entityId.toString());
        final Collection<OverdueLoanScheduleData> loanData = this.loanReadPlatformService
                .retrieveOverdueInstallmentsByLoanId(penaltyWaitPeriodValue, backdatePenalties, entityId);
        
        LOG.debug("Job {} to Loan Id {} : {} items", batchJobName, entityId, loanData.size());
        this.loanWritePlatformService.applyOverdueChargesForLoan(entityId, loanData);

        MessageBatchDataResponse response = new MessageBatchDataResponse(batchJobName, tenantIdentifier, entityId, true, null);
        return response;
    }
}
