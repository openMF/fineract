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
package org.apache.fineract.portfolio.loanorigination.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.portfolio.loanaccount.service.LoanOriginatorLinkingService;
import org.apache.fineract.portfolio.loanorigination.domain.LoanOriginator;
import org.apache.fineract.portfolio.loanorigination.domain.LoanOriginatorRepository;
import org.apache.fineract.portfolio.loanorigination.domain.WorkingCapitalLoanOriginatorMapping;
import org.apache.fineract.portfolio.loanorigination.domain.WorkingCapitalLoanOriginatorMappingRepository;
import org.apache.fineract.portfolio.loanorigination.serialization.LoanApplicationOriginatorDataValidator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/**
 * Implementation of {@link LoanOriginatorLinkingService} that handles processing of originators during loan
 * application. This service is active only when the loan-origination module is enabled.
 */
@Slf4j
@Service("workingCapitalLoanOriginatorLinkingServiceImpl")
@ConditionalOnProperty(value = "fineract.module.loan-origination.enabled", havingValue = "true")
public class WorkingCapitalLoanOriginatorLinkingServiceImpl extends AbstractLoanOriginatorLinkingServiceImpl {

    private final WorkingCapitalLoanOriginatorMappingRepository loanOriginatorMappingRepository;

    public WorkingCapitalLoanOriginatorLinkingServiceImpl(LoanOriginatorRepository loanOriginatorRepository,
            LoanApplicationOriginatorDataValidator validator, LoanOriginatorHelper loanOriginatorHelper,
            WorkingCapitalLoanOriginatorMappingRepository loanOriginatorMappingRepository) {
        super(loanOriginatorRepository, validator, loanOriginatorHelper);
        this.loanOriginatorMappingRepository = loanOriginatorMappingRepository;
    }

    @Override
    protected void createAndSaveOriginatorMapping(Long loanId, Long originatorId) {
        if (!loanOriginatorMappingRepository.existsByLoanIdAndOriginatorId(loanId, originatorId)) {
            final LoanOriginator originatorRef = loanOriginatorRepository.getReferenceById(originatorId);
            final WorkingCapitalLoanOriginatorMapping mapping = WorkingCapitalLoanOriginatorMapping.create(loanId, originatorRef);
            loanOriginatorMappingRepository.save(mapping);
            log.debug("Attached originator {} to working capital loan {}", originatorId, loanId);
        }
    }
}
