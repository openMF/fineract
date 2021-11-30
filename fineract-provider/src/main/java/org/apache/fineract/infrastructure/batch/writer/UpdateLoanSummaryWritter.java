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

import java.util.ArrayList;
import java.util.List;

import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResults;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.apache.fineract.scheduledjobs.service.ScheduledJobRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile(JobConstants.SPRING_BATCH_PROFILE_NAME)
public class UpdateLoanSummaryWritter extends BatchWriterBase implements ItemWriter<Long>, StepExecutionListener {

    public static final Logger LOG = LoggerFactory.getLogger(UpdateLoanSummaryWritter.class);
    
    @Autowired
    private ScheduledJobRunnerService scheduledJobRunnerService;
    
    public UpdateLoanSummaryWritter() {
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        initialize(stepExecution);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        LOG.info("==={}=== {} {} with items processed {}", batchJobInstanceId.get(), batchStepName.get(), 
            stepExecution.getExitStatus().getExitCode(), processed.get());
        cleanup();
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(List<? extends Long> items) throws Exception {
        List<Long> loanIds = new ArrayList<>(items);

        final int totalItems = items.size();    
        processed.set(scheduledJobRunnerService.updateLoanSummaryDetails(loanIds));

        MessageBatchDataResults message = new MessageBatchDataResults(tenantIdentifier.get(), batchStepName.get(), totalItems, processed.get(), (totalItems - processed.get()), true);
        sendMessage(message);
    }
}
