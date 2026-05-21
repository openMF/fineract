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
package org.apache.fineract.portfolio.collateralmanagement.starter;

import org.apache.fineract.infrastructure.codes.domain.CodeValueRepositoryWrapper;
import org.apache.fineract.infrastructure.core.serialization.FromJsonHelper;
import org.apache.fineract.organisation.monetary.domain.ApplicationCurrencyRepository;
import org.apache.fineract.portfolio.client.domain.ClientRepositoryWrapper;
import org.apache.fineract.portfolio.collateralmanagement.domain.ClientCollateralManagementRepositoryWrapper;
import org.apache.fineract.portfolio.collateralmanagement.domain.CollateralManagementRepositoryWrapper;
import org.apache.fineract.portfolio.collateralmanagement.service.ClientCollateralManagementReadService;
import org.apache.fineract.portfolio.collateralmanagement.service.ClientCollateralManagementReadServiceImpl;
import org.apache.fineract.portfolio.collateralmanagement.service.ClientCollateralManagementWriteService;
import org.apache.fineract.portfolio.collateralmanagement.service.ClientCollateralManagementWriteServiceImpl;
import org.apache.fineract.portfolio.collateralmanagement.service.CollateralManagementReadService;
import org.apache.fineract.portfolio.collateralmanagement.service.CollateralManagementReadServiceImpl;
import org.apache.fineract.portfolio.collateralmanagement.service.CollateralManagementWriteService;
import org.apache.fineract.portfolio.collateralmanagement.service.CollateralManagementWriteServiceImpl;
import org.apache.fineract.portfolio.collateralmanagement.service.LoanCollateralAssembler;
import org.apache.fineract.portfolio.collateralmanagement.service.LoanCollateralManagementReadService;
import org.apache.fineract.portfolio.collateralmanagement.service.LoanCollateralManagementReadServiceImpl;
import org.apache.fineract.portfolio.collateralmanagement.service.LoanCollateralManagementWriteService;
import org.apache.fineract.portfolio.collateralmanagement.service.LoanCollateralManagementWriteServiceImpl;
import org.apache.fineract.portfolio.loanaccount.domain.LoanCollateralManagementRepository;
import org.apache.fineract.portfolio.loanaccount.domain.LoanRepository;
import org.apache.fineract.portfolio.loanaccount.domain.LoanTransactionRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CollateralManagementConfiguration {

    @Bean
    @ConditionalOnMissingBean(ClientCollateralManagementReadService.class)
    public ClientCollateralManagementReadService clientCollateralManagementReadService(
            ClientCollateralManagementRepositoryWrapper clientCollateralManagementRepositoryWrapper,
            LoanTransactionRepository loanTransactionRepository) {
        return new ClientCollateralManagementReadServiceImpl(clientCollateralManagementRepositoryWrapper, loanTransactionRepository);
    }

    @Bean
    @ConditionalOnMissingBean(ClientCollateralManagementWriteService.class)
    public ClientCollateralManagementWriteService clientCollateralManagementWriteService(
            ClientCollateralManagementRepositoryWrapper clientCollateralManagementRepositoryWrapper,
            CollateralManagementRepositoryWrapper collateralManagementRepositoryWrapper, ClientRepositoryWrapper clientRepositoryWrapper) {
        return new ClientCollateralManagementWriteServiceImpl(clientCollateralManagementRepositoryWrapper,
                collateralManagementRepositoryWrapper, clientRepositoryWrapper);
    }

    @Bean
    @ConditionalOnMissingBean(CollateralManagementReadService.class)
    public CollateralManagementReadService collateralManagementReadService(
            CollateralManagementRepositoryWrapper collateralManagementRepositoryWrapper) {
        return new CollateralManagementReadServiceImpl(collateralManagementRepositoryWrapper);
    }

    @Bean
    @ConditionalOnMissingBean(CollateralManagementWriteService.class)
    public CollateralManagementWriteService collateralManagementWriteService(
            CollateralManagementRepositoryWrapper collateralManagementRepositoryWrapper,
            ApplicationCurrencyRepository applicationCurrencyRepository) {
        return new CollateralManagementWriteServiceImpl(collateralManagementRepositoryWrapper, applicationCurrencyRepository);
    }

    @Bean
    @ConditionalOnMissingBean(LoanCollateralAssembler.class)
    public LoanCollateralAssembler loanCollateralAssembler(FromJsonHelper fromApiJsonHelper, CodeValueRepositoryWrapper codeValueRepository,
            LoanCollateralManagementRepository loanCollateralRepository,
            ClientCollateralManagementRepositoryWrapper clientCollateralManagementRepositoryWrapper) {
        return new LoanCollateralAssembler(fromApiJsonHelper, codeValueRepository, loanCollateralRepository,
                clientCollateralManagementRepositoryWrapper);
    }

    @Bean
    @ConditionalOnMissingBean(LoanCollateralManagementReadService.class)
    public LoanCollateralManagementReadService loanCollateralManagementReadService(
            LoanCollateralManagementRepository loanCollateralManagementRepository, LoanRepository loanRepository) {
        return new LoanCollateralManagementReadServiceImpl(loanCollateralManagementRepository, loanRepository);
    }

    @Bean
    @ConditionalOnMissingBean(LoanCollateralManagementWriteService.class)
    public LoanCollateralManagementWriteService loanCollateralManagementWriteService(
            LoanCollateralManagementRepository loanCollateralManagementRepository,
            ClientCollateralManagementRepositoryWrapper clientCollateralManagementRepositoryWrapper) {
        return new LoanCollateralManagementWriteServiceImpl(loanCollateralManagementRepository,
                clientCollateralManagementRepositoryWrapper);
    }
}
