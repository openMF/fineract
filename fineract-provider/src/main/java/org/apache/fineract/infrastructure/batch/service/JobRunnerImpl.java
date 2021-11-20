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

    @Autowired
    public JobRunnerImpl(@Qualifier("batchJobLauncher") JobLauncher jobLauncher, final JobBuilderFactory jobBuilderFactory,
            JobExecutionListener jobExecutionListener, final ConfigurationDomainService configurationDomainService,
            final Step batchForLoansStep, final Step applyChargeForOverdueLoansStep, final Step autopayLoansStep) {
        this.jobLauncher = jobLauncher;
        this.jobBuilderFactory = jobBuilderFactory;
        this.jobExecutionListener = jobExecutionListener;

        this.configurationDomainService = configurationDomainService;
        // Steps
        this.applyChargeForOverdueLoansStep = applyChargeForOverdueLoansStep;
        this.batchForLoansStep = batchForLoansStep;
        this.autopayLoansStep = autopayLoansStep;
    }

    @Override
    public Long runJob(final Long jobId) {
        LOG.debug("runJob ===== " + jobId);
        try {
            JobExecution jobExecution = this.jobLauncher.run(getJobById(jobId), getJobParametersById(jobId));
            return jobExecution.getJobInstance().getId();
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
    public Long runJob(final Long jobId, final String parameter) {
        LOG.debug("runJob ===== " + jobId);
        try {
            JobExecution jobExecution = this.jobLauncher.run(getJobById(jobId), getJobParametersById(jobId, parameter));
            return jobExecution.getJobInstance().getId();
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
                    .end().build();
            default:
                return null;
        }
    }

    private JobParameters getJobParametersById(final Long jobId) {
        return getJobParametersById(jobId, null);
    }

    private JobParameters getJobParametersById(final Long jobId, final String parameter) {
        final FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_TENANT_ID, tenant.getTenantIdentifier());
        final String dateOfTenant = DateUtils.formatDate(DateUtils.getDateOfTenant(), BatchConstants.DEFAULT_BATCH_DATE_FORMAT);
        jobParametersBuilder.addString(BatchConstants.JOB_PARAM_TENANT_DATE, dateOfTenant);
        jobParametersBuilder.addString("instance_id", UUID.randomUUID().toString(), true);
        if (parameter != null) {
            LOG.debug("Adding parameters {}", parameter);
            jobParametersBuilder.addString(BatchConstants.JOB_PARAM_PARAMETER, parameter);
        }

        switch (jobId.intValue()) {
            // Batch Loan First Step
            case 2:
                final Long penaltyWaitPeriodValue = this.configurationDomainService.retrievePenaltyWaitPeriod();
                final Boolean backdatePenalties = this.configurationDomainService.isBackdatePenaltiesEnabled();
                LOG.debug("    penaltyWaitPeriodValue: " + penaltyWaitPeriodValue);
                LOG.debug("    backdatePenalties:      " + backdatePenalties);
                jobParametersBuilder.addLong(BatchConstants.JOB_PARAM_PENALTY_WAIT_PERIOD, penaltyWaitPeriodValue);
                jobParametersBuilder.addString(BatchConstants.JOB_PARAM_BACKDATE_PENALTIES, backdatePenalties.toString(),
                        backdatePenalties);
        }
        return jobParametersBuilder.toJobParameters();
    }

}
