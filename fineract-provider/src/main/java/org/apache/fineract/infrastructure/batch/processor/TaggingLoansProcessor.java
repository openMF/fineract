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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResponse;
import org.apache.fineract.infrastructure.batch.data.process.LoanTagsData;
import org.apache.fineract.infrastructure.codes.domain.CodeValue;
import org.apache.fineract.infrastructure.codes.domain.CodeValueRepositoryWrapper;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.dataqueries.service.ReadWriteNonCoreDataService;
import org.apache.fineract.portfolio.loanaccount.domain.LoanRepaymentScheduleInstallmentRepository;
import org.apache.fineract.portfolio.loanaccount.domain.LoanRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

public class TaggingLoansProcessor extends BatchProcessorBase implements ItemProcessor<Long, MessageBatchDataResponse> {

    public static final Logger LOG = LoggerFactory.getLogger(TaggingLoansProcessor.class);

    @Autowired
    private CodeValueRepositoryWrapper codeValueRepository;

    @Autowired
    private LoanRepository loanRepository;

    @Autowired
    private LoanRepaymentScheduleInstallmentRepository loanRepaymentScheduleInstallmentRepository;

    @Autowired
    private ReadWriteNonCoreDataService readWriteNonCoreDataService;

    private DateTimeFormatter dateTimeFormatter;

    private List<CodeValue> loanAccountState;
    private List<CodeValue> deliquent;

    @BeforeStep
    public void before(StepExecution stepExecution) {
        super.initialize(stepExecution);
        LOG.debug("Job Step {} : Tenant {}", this.batchStepName, this.tenant.getName());
        // Particular process parameters or properties
        this.dateTimeFormatter = DateTimeFormatter
            .ofPattern(BatchConstants.DEFAULT_BATCH_DATETIME_FORMAT);
        this.loanAccountState = this.codeValueRepository
            .findByCodeNameWithNotFoundDetection(BatchConstants.LOAN_ACCOUNT_STATUS_CODE);
        this.deliquent = this.codeValueRepository
            .findByCodeNameWithNotFoundDetection(BatchConstants.LOAN_DELIQUENCY_CODE);
    }

    @AfterStep
    public void after(StepExecution stepExecution) {
        LOG.debug(stepExecution.getSummary());
    }
    
    @Override
    public MessageBatchDataResponse process(Long entityId) throws Exception {
        
        final LocalDateTime taggedAtDateTime = DateUtils.getLocalDateTimeOfTenant();
        final String taggedAt = taggedAtDateTime.format(dateTimeFormatter);
        final boolean paidOff = false;

        final Long accountStatus = this.getAccountStatus(entityId);
        final Integer deliquenLevel = this.getDeliquenLevel(entityId);
        boolean chargedOff = false;
        Long deliquency = null;
        if (deliquenLevel != null) {
            final String deliquenLabel = "Delinquent " + deliquenLevel;
            deliquency = this.codeByValue(this.deliquent, deliquenLabel);
            chargedOff = (deliquenLevel >= 30 && deliquenLevel <= 120);    
        }
        
        final LoanTagsData loanTagsData = new LoanTagsData(entityId, taggedAt, chargedOff, 
            paidOff, accountStatus, deliquency);
        final String jsonData = loanTagsData.toJson();
        // LOG.debug("Json {} ", jsonData);

        final CommandProcessingResult result = this.readWriteNonCoreDataService.createNewDatatableEntry(
            BatchConstants.LOAN_TAGS_DATATABLE, entityId, jsonData, this.appUser);
        final MessageBatchDataResponse response = new MessageBatchDataResponse(batchStepName, 
            tenantIdentifier, entityId, true, true, result.commandId());
        return response;
    }

    private Long getAccountStatus(Long entityId) {
        final Integer loanAccountStatus = this.loanRepository.fetchLoanStatusById(entityId);
        if (loanAccountStatus >= 300 && loanAccountStatus <= 400)
            return codeByValue(this.loanAccountState, "ACTIVE");
        else if (loanAccountStatus >= 400 && loanAccountStatus < 600)
            return codeByValue(this.loanAccountState, "CANCELED");
        else if (loanAccountStatus >= 600)
            return codeByValue(this.loanAccountState, "CLOSED");
        else
            return codeByValue(this.loanAccountState, "PENDING");
    }

    private Integer getDeliquenLevel(Long entityId) {
        final Date repaymentDate = this.loanRepaymentScheduleInstallmentRepository
            .fetchLoanRepaymentScheduleInstallmentUnpaidByLoanId(entityId);
        if (repaymentDate == null)
            return null;

        final Long daysInBetween = DateUtils.getDaysInBetween(repaymentDate, dateOfTenant);
        Integer deliquenLevel = 1;
        if (daysInBetween > 3 && daysInBetween <= 30)
            deliquenLevel = 3;
        else if (daysInBetween > 3 && daysInBetween <= 30)
            deliquenLevel = 3;
        else if (daysInBetween > 30 && daysInBetween <= 60)
            deliquenLevel = 30;
        else if (daysInBetween > 60 && daysInBetween <= 90)
            deliquenLevel = 60;
        else if (daysInBetween > 90 && daysInBetween <= 120)
            deliquenLevel = 90;
        else if (daysInBetween > 120 && daysInBetween <= 150)
            deliquenLevel = 120;
        else if (daysInBetween > 150 && daysInBetween <= 180)
            deliquenLevel = 150;
        else if (daysInBetween > 150 && daysInBetween <= 180)
            deliquenLevel = 180;
        else if (daysInBetween > 210 && daysInBetween <= 240)
            deliquenLevel = 210;
        else if (daysInBetween > 240)
            deliquenLevel = 240;

        return deliquenLevel;
    }

    private Long codeByValue(List<CodeValue> codes, String value) {
        if (value == null)
            return null;
        for (CodeValue code : codes) {
            if (code.isLabel(value)) 
                return code.getId();
        }
        return null;
    }
}
