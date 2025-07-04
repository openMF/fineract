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
package org.apache.fineract.cob.savings;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.cob.COBBusinessStep;
import org.apache.fineract.cob.COBBusinessStepService;
import org.apache.fineract.cob.data.AccountCOBParameter;
import org.apache.fineract.cob.data.AccountCOBPartition;
import org.apache.fineract.cob.data.BusinessStepNameAndOrder;
import org.apache.fineract.infrastructure.jobs.service.JobName;
import org.apache.fineract.infrastructure.springbatch.PropertyService;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.util.StopWatch;

@Slf4j
@RequiredArgsConstructor
public class SavingsCOBPartitioner implements Partitioner {

    public static final String PARTITION_PREFIX = "partition_";

    private final PropertyService propertyService;
    private final COBBusinessStepService cobBusinessStepService;
    private final RetrieveSavingsIdService retrieveSavingsIdService;
    private final JobOperator jobOperator;
    private final JobExplorer jobExplorer;

    private final Long numberOfDays;

    @Value("#{stepExecutionContext['BusinessDate']}")
    @Setter
    private LocalDate businessDate;
    @Value("#{stepExecutionContext['IS_CATCH_UP']}")
    @Setter
    private Boolean isCatchUp;

    @NonNull
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int partitionSize = propertyService.getPartitionSize(SavingsCOBConstant.JOB_NAME);
        Set<BusinessStepNameAndOrder> cobBusinessSteps = cobBusinessStepService.getCOBBusinessSteps(COBBusinessStep.class,
                SavingsCOBConstant.SAVINGS_COB_JOB_NAME);
        return getPartitions(partitionSize, cobBusinessSteps);
    }

    private Map<String, ExecutionContext> getPartitions(int partitionSize, Set<BusinessStepNameAndOrder> cobBusinessSteps) {
        if (cobBusinessSteps.isEmpty()) {
            stopJobExecution();
            return Map.of();
        }
        StopWatch sw = new StopWatch();
        sw.start();
        List<AccountCOBPartition> savingsCOBPartitions = new ArrayList<>(retrieveSavingsIdService.retrieveSavingsCOBPartitions(numberOfDays,
                businessDate, isCatchUp != null && isCatchUp, partitionSize));
        sw.stop();
        // if there is no savings to be closed, we still would like to create at least one partition

        if (savingsCOBPartitions.size() == 0) {
            savingsCOBPartitions.add(new AccountCOBPartition(0L, 0L, 1L, 0L));
        }
        log.info(
                "AccountCOBPartitioner found {} savings to be processed as part of COB. {} partitions were created using partition size {}. RetrieveAccountCOBPartitions was executed in {} ms.",
                getSavingsCount(savingsCOBPartitions), savingsCOBPartitions.size(), partitionSize, sw.getTotalTimeMillis());
        return savingsCOBPartitions.stream()
                .collect(Collectors.toMap(l -> PARTITION_PREFIX + l.getPageNo(), l -> createNewPartition(cobBusinessSteps, l)));
    }

    private long getSavingsCount(List<AccountCOBPartition> savingsCOBPartitions) {
        return savingsCOBPartitions.stream().map(AccountCOBPartition::getCount).reduce(0L, Long::sum);
    }

    private ExecutionContext createNewPartition(Set<BusinessStepNameAndOrder> cobBusinessSteps, AccountCOBPartition savingsCOBPartition) {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.put(SavingsCOBConstant.BUSINESS_STEPS, cobBusinessSteps);
        executionContext.put(SavingsCOBConstant.SAVINGS_COB_PARAMETER,
                new AccountCOBParameter(savingsCOBPartition.getMinId(), savingsCOBPartition.getMaxId()));
        executionContext.put("partition", PARTITION_PREFIX + savingsCOBPartition.getPageNo());
        return executionContext;
    }

    private void stopJobExecution() {
        Set<JobExecution> runningJobExecutions = jobExplorer.findRunningJobExecutions(JobName.SAVINGS_COB.name());
        for (JobExecution jobExecution : runningJobExecutions) {
            try {
                jobOperator.stop(jobExecution.getId());
            } catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
                log.error("There is no running execution for the given execution ID. Execution ID: {}", jobExecution.getId());
                throw new RuntimeException(e);
            }
        }
    }
}
