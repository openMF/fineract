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

import java.util.Iterator;
import java.util.List;
import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.service.BatchJobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.stereotype.Component;

@Component
public class BlockLoansReader implements ItemReader<Long> {

    public static final Logger LOG = LoggerFactory.getLogger(BlockLoansReader.class);
    private static final ThreadLocal<Iterator<Long>> dataIterator = new ThreadLocal<>();

    @BeforeStep
    public void before(StepExecution stepExecution) {
        final String batchStepName = stepExecution.getStepName();
        JobParameters parameters = stepExecution.getJobExecution().getJobParameters();
        final String parameter = parameters.getString(BatchConstants.JOB_PARAM_PARAMETER);
        List<Long> loanIds = BatchJobUtils.getLoanIds(parameter);
        dataIterator.set(loanIds.iterator());
        LOG.debug("{} - {} processing {} Loan Ids", batchStepName, stepExecution.getId(), loanIds.size());
    }

    @Override
    public Long read() {
        if (dataIterator.get() != null && dataIterator.get().hasNext()) {
            return dataIterator.get().next();
        } else {
            return null;
        }
    }
}
