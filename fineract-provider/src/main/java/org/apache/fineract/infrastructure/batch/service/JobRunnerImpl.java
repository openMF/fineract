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
package org.apache.fineract.infrastructure.batch.service;

import java.util.UUID;
import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataRequest;
import org.apache.fineract.infrastructure.batch.data.MessageJobResponse;
import org.apache.fineract.infrastructure.batch.exception.JobAlreadyCompletedException;
import org.apache.fineract.infrastructure.batch.exception.JobAlreadyRunningException;
import org.apache.fineract.infrastructure.batch.exception.JobIllegalRestartException;
import org.apache.fineract.infrastructure.batch.exception.JobParameterInvalidException;
import org.apache.fineract.infrastructure.configuration.domain.ConfigurationDomainService;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile(JobConstants.SPRING_BATCH_PROFILE_NAME)
public class JobRunnerImpl implements JobRunner {

    public static final Logger LOG = LoggerFactory.getLogger(JobRunnerImpl.class);

    private final JobLauncher jobLauncher;
    private final JobBuilderFactory jobBuilderFactory;
    private final JobExecutionListener jobExecutionListener;
    private final ConfigurationDomainService configurationDomainService;

    // Steps
    private final Step batchForLoansStep;
    private final Step autopayLoansStep;
    private final Step applyChargeForOverdueLoansStep;
    private final Step taggingLoansStep;
    private final Step updateLoanArrearsAgingStep;
    private final Step updateLoanSummaryStep;

    @Autowired
    public JobRunnerImpl(@Qualifier("batchJobLauncher") JobLauncher jobLauncher, final JobBuilderFactory jobBuilderFactory,
            JobExecutionListener jobExecutionListener, final ConfigurationDomainService configurationDomainService,
            final Step batchForLoansStep, final Step autopayLoansStep, final Step applyChargeForOverdueLoansStep,
            final Step taggingLoansStep, final Step updateLoanArrearsAgingStep, final Step updateLoanSummaryStep) {
        this.jobLauncher = jobLauncher;
        this.jobBuilderFactory = jobBuilderFactory;
        this.jobExecutionListener = jobExecutionListener;

        this.configurationDomainService = configurationDomainService;
        // Steps
        this.applyChargeForOverdueLoansStep = applyChargeForOverdueLoansStep;
        this.batchForLoansStep = batchForLoansStep;
        this.autopayLoansStep = autopayLoansStep;
        this.taggingLoansStep = taggingLoansStep;
        this.updateLoanArrearsAgingStep = updateLoanArrearsAgingStep;
        this.updateLoanSummaryStep = updateLoanSummaryStep;
    }

    @Override
    public MessageJobResponse runChunkJob(final String cobDate, final Long limit) {
        try {
            final String uuid = UUID.randomUUID().toString();
            JobExecution jobExecution = this.jobLauncher.run(getJobById(BatchConstants.BATCH_CHUNKJOB_PROCESS_ID), 
                getChunkerJobParameters(uuid, cobDate, limit));
            final Long jobInstanceId = jobExecution.getJobInstance().getInstanceId();
            MessageJobResponse response = new MessageJobResponse(jobInstanceId, uuid);

            LOG.debug("runChunkJob ===== {}", jobInstanceId);
            return response;
        } catch (JobExecutionAlreadyRunningException e) {
            throw new JobAlreadyRunningException();
        } catch (JobInstanceAlreadyCompleteException e) {
            throw new JobAlreadyCompletedException();
        } catch (JobParametersInvalidException e) {
            throw new JobParameterInvalidException();
        } catch (JobRestartException e) {
            throw new JobIllegalRestartException();
        }
    }

    @Override
    public MessageJobResponse runCOBJob(MessageBatchDataRequest messageData) {
        try {
            JobExecution jobExecution = this.jobLauncher.run(
                getJobById(BatchConstants.BATCH_COBJOB_PROCESS_ID), 
                getCOBJobParameters(messageData));
            Long jobInstanceId = jobExecution.getJobInstance().getInstanceId();
            MessageJobResponse response = new MessageJobResponse(jobInstanceId, messageData.getJobInstanceId(), messageData.getIdentifier());

            LOG.debug("runCOBJob ===== {} : {}", messageData.getJobInstanceId(), jobInstanceId);
            return response;
        } catch (JobExecutionAlreadyRunningException e) {
            throw new JobAlreadyRunningException();
        } catch (JobInstanceAlreadyCompleteException e) {
            throw new JobAlreadyCompletedException();
        } catch (JobParametersInvalidException e) {
            throw new JobParameterInvalidException();
        } catch (JobRestartException e) {
            throw new JobIllegalRestartException();
        }
    }

    @Override
    public void stopJob(final Long jobId) {
        LOG.debug("stopJob ===== " + jobId);
    }

    private Job getJobById(final Long jobId) {
        switch (jobId.intValue()) {
            case 1:
                return jobBuilderFactory.get(BatchConstants.BATCH_BLOCKLOANS_JOB_NAME)
                    .listener(jobExecutionListener).flow(batchForLoansStep)
                    .end().build();
            case 2:
                return jobBuilderFactory.get(BatchConstants.BATCH_COB_JOB_NAME)
                    .listener(jobExecutionListener).flow(autopayLoansStep)
                    .next(applyChargeForOverdueLoansStep)
                    .next(taggingLoansStep)
                    .next(updateLoanArrearsAgingStep)
                    .next(updateLoanSummaryStep)
                    .end().build();
            default:
                return null;
        }
    }

    private JobParameters getChunkerJobParameters(String uuid, String cobDate, Long limit) {
        final FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_TENANT_ID, tenant.getTenantIdentifier());
        
        final String dateOfTenant = DateUtils.formatDate(DateUtils.getDateOfTenant(), BatchConstants.DEFAULT_BATCH_DATE_FORMAT);
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_TENANT_DATE, dateOfTenant);
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_INSTANCE_ID, uuid, true);

        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_COB_DATE, cobDate);
        jobParametersBuilder.addLong(BatchConstants.JOB_PARAM_LIMIT_READ, limit);
        return jobParametersBuilder.toJobParameters();
    }

    private JobParameters getCOBJobParameters(MessageBatchDataRequest messageData) {
        final FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_TENANT_ID, tenant.getTenantIdentifier());
        
        final String dateOfTenant = DateUtils.formatDate(DateUtils.getDateOfTenant(), BatchConstants.DEFAULT_BATCH_DATE_FORMAT);
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_TENANT_DATE, dateOfTenant);
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_INSTANCE_ID, messageData.getIdentifier() , true);
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_COB_DATE, messageData.getCobDate());
        jobParametersBuilder.addLong(BatchConstants.JOB_PARAM_PARENT_ID, messageData.getJobInstanceId());
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_PARAMETER, messageData.getEntityIdsAsString());

        final Long penaltyWaitPeriodValue = this.configurationDomainService.retrievePenaltyWaitPeriod();
        final Boolean backdatePenalties = this.configurationDomainService.isBackdatePenaltiesEnabled();
        jobParametersBuilder.addLong(BatchConstants.JOB_PARAM_PENALTY_WAIT_PERIOD, penaltyWaitPeriodValue);
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_BACKDATE_PENALTIES, backdatePenalties.toString(),
                backdatePenalties);
        return jobParametersBuilder.toJobParameters();
    }
}
