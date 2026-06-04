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

package org.apache.fineract.portfolio.workingcapitalloan.service;

import java.math.BigDecimal;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResultBuilder;
import org.apache.fineract.infrastructure.core.domain.ExternalId;
import org.apache.fineract.infrastructure.core.exception.PlatformApiDataValidationException;
import org.apache.fineract.infrastructure.core.service.ExternalIdFactory;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.portfolio.charge.domain.Charge;
import org.apache.fineract.portfolio.charge.domain.ChargeRepositoryWrapper;
import org.apache.fineract.portfolio.charge.domain.ChargeTimeType;
import org.apache.fineract.portfolio.workingcapitalloan.WorkingCapitalLoanConstants;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoan;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanBalance;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanCharge;
import org.apache.fineract.portfolio.workingcapitalloan.exception.WorkingCapitalLoanNotFoundException;
import org.apache.fineract.portfolio.workingcapitalloan.repository.WorkingCapitalLoanBalanceRepository;
import org.apache.fineract.portfolio.workingcapitalloan.repository.WorkingCapitalLoanChargeRepository;
import org.apache.fineract.portfolio.workingcapitalloan.repository.WorkingCapitalLoanRepository;
import org.apache.fineract.portfolio.workingcapitalloan.serialization.WorkingCapitalLoanChargeConstants;
import org.apache.fineract.portfolio.workingcapitalloan.serialization.WorkingCapitalLoanChargeDataValidator;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class WorkingCapitalLoanChargeWritePlatformServiceImpl implements WorkingCapitalLoanChargeWritePlatformService {

    private final WorkingCapitalLoanChargeDataValidator loanChargeDataValidator;
    private final WorkingCapitalLoanRepository workingCapitalLoanRepository;
    private final ChargeRepositoryWrapper chargeRepository;
    private final WorkingCapitalLoanChargeRepository loanChargeRepository;
    private final ExternalIdFactory externalIdFactory;
    private final WorkingCapitalLoanBalanceRepository balanceRepository;

    @Override
    public CommandProcessingResult createLoanCharge(Long loanId, JsonCommand command) {
        loanChargeDataValidator.validateCreateLoanCharge(command.json());
        WorkingCapitalLoan loan = workingCapitalLoanRepository.findById(loanId)
                .orElseThrow(() -> new WorkingCapitalLoanNotFoundException(loanId));

        WorkingCapitalLoanCharge loanCharge = assemblyChargeFromCommand(loan, command);

        loanCharge = loanChargeRepository.saveAndFlush(loanCharge);

        addChargeToBalance(loan, loanCharge);

        return new CommandProcessingResultBuilder() //
                .withCommandId(command.commandId()) //
                .withEntityId(loanCharge.getId()) //
                .withEntityExternalId(loanCharge.getExternalId()) //
                .withOfficeId(loan.getOfficeId()) //
                .withClientId(loan.getClientId()) //
                .withLoanId(loanId) //
                .build();
    }

    private WorkingCapitalLoanCharge assemblyChargeFromCommand(WorkingCapitalLoan loan, JsonCommand command) {
        final BigDecimal amount = command.bigDecimalValueOfParameterNamed("amount");
        final LocalDate dueDate = command.dateValueOfParameterNamed("dueDate");
        final Long chargeId = command.longValueOfParameterNamed("chargeId");
        final ExternalId externalId = externalIdFactory.createFromCommand(command, WorkingCapitalLoanConstants.externalIdParameterName);

        final Charge chargeDefinition = chargeRepository.findOneWithNotFoundDetection(chargeId);
        if (ChargeTimeType.SPECIFIED_DUE_DATE.getValue().equals(chargeDefinition.getChargeTimeType())) {
            if (dueDate == null) {
                throw new PlatformApiDataValidationException("field.is.mandatory", "Field is mandatory",
                        WorkingCapitalLoanChargeConstants.dueDateParamName);
            }
            if (dueDate.isBefore(ThreadLocalContextUtil.getBusinessDate())) {
                throw new PlatformApiDataValidationException("dueDate.cannot.be.in.the.past", "DueDate cannot be in the past",
                        WorkingCapitalLoanChargeConstants.dueDateParamName);
            }
            if (!loan.getLoanStatus().isActive()) {
                throw new PlatformApiDataValidationException("loan.should.be.active", "Loan should be in active status",
                        "workingCapitalLoan");
            }
        }
        return WorkingCapitalLoanCharge.build(loan, externalId, chargeDefinition, amount, dueDate,
                ThreadLocalContextUtil.getBusinessDate());
    }

    private void addChargeToBalance(WorkingCapitalLoan loan, WorkingCapitalLoanCharge loanCharge) {
        final WorkingCapitalLoanBalance balance = balanceRepository.findByWcLoan_Id(loan.getId())
                .orElseGet(() -> WorkingCapitalLoanBalance.createFor(loan));

        if (loanCharge.isPenaltyCharge()) {
            balance.setPenalty(balance.getPenalty().add(loanCharge.getAmount()));
        } else {
            balance.setFee(balance.getFee().add(loanCharge.getAmount()));
        }
    }
}
