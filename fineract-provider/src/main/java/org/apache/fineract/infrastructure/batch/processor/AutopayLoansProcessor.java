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

import java.util.List;

import com.google.gson.JsonElement;

import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResponse;
import org.apache.fineract.infrastructure.batch.data.process.LoanRepaymentData;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import org.apache.fineract.infrastructure.core.serialization.FromJsonHelper;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.apache.fineract.organisation.monetary.domain.Money;
import org.apache.fineract.portfolio.loanaccount.domain.Loan;
import org.apache.fineract.portfolio.loanaccount.domain.LoanRepaymentScheduleInstallment;
import org.apache.fineract.portfolio.loanaccount.domain.LoanRepaymentScheduleInstallmentRepository;
import org.apache.fineract.portfolio.loanaccount.service.LoanWritePlatformServiceJpaRepositoryImpl;
import org.apache.fineract.portfolio.paymenttype.domain.PaymentType;
import org.apache.fineract.portfolio.paymenttype.domain.PaymentTypeRepositoryWrapper;
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
public class AutopayLoansProcessor extends BatchProcessorBase implements ItemProcessor<Long, MessageBatchDataResponse> {

    public static final Logger LOG = LoggerFactory.getLogger(AutopayLoansProcessor.class);

    @Autowired
    private LoanRepaymentScheduleInstallmentRepository loanRepaymentScheduleInstallmentRepository;

    @Autowired
    private LoanWritePlatformServiceJpaRepositoryImpl loanWritePlatformService;

    @Autowired
    private FromJsonHelper fromApiJsonHelper;

    @Autowired
    private PaymentTypeRepositoryWrapper paymentTypeRepository;

    private PaymentType autopayPaymentType;

    @BeforeStep
    public void before(StepExecution stepExecution) {
        super.initialize(stepExecution);
        LOG.debug("Job Step {} : Tenant {}", this.batchStepName, this.tenant.getName());
        // Particular process parameters or properties
        this.autopayPaymentType = this.paymentTypeRepository.findByName("AUTOPAY");
    }

    @AfterStep
    public void after(StepExecution stepExecution) {
        LOG.debug("{} items processed {}", this.batchStepName, this.processed);
    }

    @Override
    public MessageBatchDataResponse process(Long entityId) throws Exception {
        final List<LoanRepaymentScheduleInstallment> repaymentInstallments = this.loanRepaymentScheduleInstallmentRepository
            .fetchLoanRepaymentScheduleInstallmentMaturedByLoanId(entityId, dateOfTenant, false);

        MessageBatchDataResponse response;
        if (!repaymentInstallments.isEmpty()) {
            // LOG.debug("Job Step {} to Loan Id {} : installment number {} : duedate {}", 
            //    batchStepName, entityId, repaymentInstallment.getInstallmentNumber(), repaymentInstallment.getDueDate());

            // Process the first Item
            LoanRepaymentScheduleInstallment repaymentInstallment = repaymentInstallments.get(0);
            final Loan loan = repaymentInstallment.getLoan();
            final Money amountDue = repaymentInstallment.getDue(loan.getCurrency());
            final String dueDateVal = DateUtils.formatDate(repaymentInstallment.getDueDateAsDate(), BatchConstants.DEFAULT_BATCH_DATE_FORMAT);
            // LOG.debug("    autopaying loan {} for due date {}", entityId, dueDateVal);
            LoanRepaymentData loanRepaymentData = new LoanRepaymentData(entityId, dueDateVal, amountDue.getAmount(), "", 
                BatchConstants.DEFAULT_BATCH_DATE_LOCALE, BatchConstants.DEFAULT_BATCH_DATE_FORMAT, this.autopayPaymentType.getId().intValue());

            final String jsonData = gson.toJson(loanRepaymentData);
            // LOG.debug("Json {} ", jsonData);
            final JsonElement parsedCommand = this.fromApiJsonHelper.parse(jsonData);

            JsonCommand command = new JsonCommand(entityId, jsonData, parsedCommand, this.fromApiJsonHelper, "LOAN", this.appUser);

            final boolean isRecoveryRepayment = false;
            final CommandProcessingResult processResult = loanWritePlatformService.makeLoanRepayment(entityId, command, isRecoveryRepayment);
            response = new MessageBatchDataResponse(batchStepName, tenantIdentifier, entityId, true, true, processResult.getTransactionId());
            this.processed++;
            // LOG.debug("processResult: {} ", processResult.getTransactionId());
        } else {
            // LOG.debug("Job Step {} to Loan Id {} : Not repayment for date {}", batchStepName, entityId, dateOfTenant);
            response = new MessageBatchDataResponse(batchStepName, tenantIdentifier, entityId, true, false, null);
        }

        return response;
    }
}
