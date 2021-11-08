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

import org.apache.fineract.infrastructure.batch.exception.JobAlreadyCompletedException;
import org.apache.fineract.infrastructure.batch.exception.JobAlreadyRunningException;
import org.apache.fineract.infrastructure.batch.exception.JobIllegalRestartException;
import org.apache.fineract.infrastructure.batch.exception.JobParameterInvalidException;
import org.apache.fineract.infrastructure.configuration.domain.ConfigurationDomainService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
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
import org.springframework.stereotype.Service;

@Service
public class JobRunnerImpl implements JobRunner {

    public static final Logger LOG = LoggerFactory.getLogger(JobRunnerImpl.class);

    private final JobLauncher batchJobLauncher;
    private final JobBuilderFactory jobBuilderFactory;
    private final JobExecutionListener jobExecutionListener;
    private final ConfigurationDomainService configurationDomainService;

    // Steps
    private final Step applyChargeForOverdueLoansStep;

    @Autowired
    public JobRunnerImpl(final JobLauncher batchJobLauncher, final JobBuilderFactory jobBuilderFactory,
            JobExecutionListener jobExecutionListener, final ConfigurationDomainService configurationDomainService,
            final Step applyChargeForOverdueLoansStep) {
        this.batchJobLauncher = batchJobLauncher;
        this.jobBuilderFactory = jobBuilderFactory;
        this.jobExecutionListener = jobExecutionListener;

        this.configurationDomainService = configurationDomainService;
        this.applyChargeForOverdueLoansStep = applyChargeForOverdueLoansStep;
    }

    @Override
    public void runJob(final Long jobId) {
        LOG.info("runJob ===== " + jobId);
        try {
            this.batchJobLauncher.run(getJobById(jobId), getJobParametersById(jobId));
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
        LOG.info("stopJob ===== " + jobId);

    }

    private Job getJobById(final Long jobId) {
        switch (jobId.intValue()) {
            case 1:
                return jobBuilderFactory.get("applyChargeForOverdueLoansJob").listener(jobExecutionListener)
                        .flow(applyChargeForOverdueLoansStep).end().build();
            default:
                return null;
        }
    }

    private JobParameters getJobParametersById(final Long jobId) {
        LOG.info("getJobParametersById ===== " + jobId);

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        switch (jobId.intValue()) {
            case 1:
                final Long penaltyWaitPeriodValue = this.configurationDomainService.retrievePenaltyWaitPeriod();
                final Boolean backdatePenalties = this.configurationDomainService.isBackdatePenaltiesEnabled();
                LOG.info("    penaltyWaitPeriodValue: " + penaltyWaitPeriodValue);
                LOG.info("    backdatePenalties:      " + backdatePenalties);
                jobParametersBuilder.addLong("penaltyWaitPeriodValue", penaltyWaitPeriodValue);
                jobParametersBuilder.addString("backdatePenalties", backdatePenalties.toString(), backdatePenalties);
        }
        return jobParametersBuilder.toJobParameters();
    }

}
