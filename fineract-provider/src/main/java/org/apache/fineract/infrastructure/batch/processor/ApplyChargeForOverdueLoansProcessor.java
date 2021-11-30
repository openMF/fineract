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

import java.time.LocalDate;
import java.util.Collection;
import java.util.Date;

import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResponse;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.core.utils.TextUtils;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.apache.fineract.portfolio.loanaccount.loanschedule.data.OverdueLoanScheduleData;
import org.apache.fineract.portfolio.loanaccount.service.LoanReadPlatformService;
import org.apache.fineract.portfolio.loanaccount.service.LoanWritePlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile(JobConstants.SPRING_BATCH_PROFILE_NAME)
public class ApplyChargeForOverdueLoansProcessor extends BatchProcessorBase implements ItemProcessor<Long, MessageBatchDataResponse> {

    public static final Logger LOG = LoggerFactory.getLogger(ApplyChargeForOverdueLoansProcessor.class);

    @Autowired
    private LoanReadPlatformService loanReadPlatformService;

    @Autowired
    private LoanWritePlatformService loanWritePlatformService;

    private final static ThreadLocal<Long> penaltyWaitPeriodValue = new ThreadLocal<>();
    private final static ThreadLocal<Boolean> backdatePenalties = new ThreadLocal<>();
    private final static ThreadLocal<LocalDate> recalculateFrom = new ThreadLocal<>();

    @BeforeStep
    public void before(StepExecution stepExecution) {
        initialize(stepExecution);
        LOG.debug("Job Step {} : Tenant {}", batchStepName.get(), tenantIdentifier.get());
        // Particular process parameters or properties
        penaltyWaitPeriodValue.set(parameters.get().getLong(BatchConstants.JOB_PARAM_PENALTY_WAIT_PERIOD));
        backdatePenalties.set(TextUtils.stringToBoolean(parameters.get().getString(BatchConstants.JOB_PARAM_BACKDATE_PENALTIES)));

        Date processDate = DateUtils.createDate(dateOfTenantValue.get(), BatchConstants.DEFAULT_BATCH_DATE_FORMAT);
        if (cobDateValue.get() != null)
        processDate = DateUtils.createDate(cobDateValue.get(), BatchConstants.DEFAULT_BATCH_DATE_FORMAT);
    
        recalculateFrom.set(DateUtils.fromDateToLocalDate(processDate));
    }

    @AfterStep
    public void after(StepExecution stepExecution) {
        LOG.debug("{} items processed {}", batchStepName.get(), processed.get());
        cleanup();
        penaltyWaitPeriodValue.remove();
        backdatePenalties.remove();
        recalculateFrom.remove();
    }
    
    @Override
    public MessageBatchDataResponse process(Long entityId) throws Exception {
        final Collection<OverdueLoanScheduleData> loanData = this.loanReadPlatformService
                .retrieveOverdueInstallmentsByLoanId(penaltyWaitPeriodValue.get(), backdatePenalties.get(), entityId);
        
        boolean changed = !loanData.isEmpty();
        if (changed) {
            // LOG.debug("Job Step {} to Loan Id {} : {} matured installments", batchStepName, entityId, loanData.size());
            this.loanWritePlatformService.applyOverdueChargesForLoan(recalculateFrom.get(), entityId, loanData, true);
            processed.set(processed.get()+1);
        }

        MessageBatchDataResponse response = new MessageBatchDataResponse(batchStepName.get(), tenantIdentifier.get(), entityId, true, changed, null);
        return response;
    }
}
