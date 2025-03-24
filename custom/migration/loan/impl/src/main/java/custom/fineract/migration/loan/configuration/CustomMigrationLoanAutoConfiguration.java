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
package custom.fineract.migration.loan.configuration;

import custom.fineract.migration.loan.serializer.CustomLoanMigrationDataSerializer;
import custom.fineract.migration.loan.serializer.CustomLoanTransactionMigrationDataSerializer;
import custom.fineract.migration.loan.service.CustomExternalBusinessEventConfigurationServiceImpl;
import custom.fineract.migration.loan.service.MigrationCompletedListener;
import custom.fineract.migration.loan.service.MigrationService;
import custom.fineract.migration.loan.service.MigrationServiceImpl;
import org.apache.fineract.infrastructure.dataqueries.service.DatatableReadService;
import org.apache.fineract.infrastructure.event.business.service.BusinessEventNotifierService;
import org.apache.fineract.infrastructure.event.business.service.ExternalBusinessEventConfigurationService;
import org.apache.fineract.infrastructure.event.external.repository.ExternalEventConfigurationRepository;
import org.apache.fineract.infrastructure.event.external.service.support.ByteBufferConverter;
import org.apache.fineract.portfolio.loanaccount.domain.LoanRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@ComponentScan("custom.fineract.migration")
@ConditionalOnProperty("custom.migration.loan.enabled")
public class CustomMigrationLoanAutoConfiguration {

    @Bean
    public CustomLoanMigrationDataSerializer customLoanMigrationDataSerializer(MigrationService migrationService,
            ByteBufferConverter byteBufferConverter) {
        return new CustomLoanMigrationDataSerializer(migrationService, byteBufferConverter);
    }

    @Bean
    public CustomLoanTransactionMigrationDataSerializer customLoanTransactionMigrationDataSerializer(MigrationService migrationService,
            ByteBufferConverter byteBufferConverter) {
        return new CustomLoanTransactionMigrationDataSerializer(migrationService, byteBufferConverter);
    }

    @Bean
    public ExternalBusinessEventConfigurationService externalBusinessEventConfigurationService(
            ExternalEventConfigurationRepository eventConfigurationRepository, MigrationService migrationService) {
        return new CustomExternalBusinessEventConfigurationServiceImpl(eventConfigurationRepository, migrationService);
    }

    @Bean
    public MigrationCompletedListener businessEventListener(BusinessEventNotifierService businessEventNotifierService,
            LoanRepository loanRepository) {
        return new MigrationCompletedListener(businessEventNotifierService, loanRepository);
    }

    @Bean
    public MigrationService migrationService(DatatableReadService datatableReadService) {
        return new MigrationServiceImpl(datatableReadService);
    }
}
