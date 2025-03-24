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
package custom.fineract.migration.loan.service;

import static custom.fineract.migration.loan.enums.MigrationStatus.MIGRATION_COMPLETED;
import static custom.fineract.migration.loan.enums.MigrationStatus.MIGRATION_IN_PROGRESS;

import custom.fineract.migration.loan.enums.MigrationStatus;
import jakarta.annotation.PostConstruct;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.event.business.domain.BusinessEvent;
import org.apache.fineract.infrastructure.event.business.service.ExternalBusinessEventConfigurationService;
import org.apache.fineract.infrastructure.event.business.service.ExternalBusinessEventConfigurationServiceImpl;
import org.apache.fineract.infrastructure.event.external.repository.ExternalEventConfigurationRepository;
import org.springframework.beans.factory.annotation.Value;

@RequiredArgsConstructor
public class CustomExternalBusinessEventConfigurationServiceImpl implements ExternalBusinessEventConfigurationService {

    private final ExternalEventConfigurationRepository eventConfigurationRepository;
    private final MigrationService migrationService;
    private ExternalBusinessEventConfigurationService fallbackExternalBusinessEventConfigurationService;

    @Value("${custom.migration.loan.migration.events}")
    private List<String> migrationEvents;

    @PostConstruct
    public void init() {
        fallbackExternalBusinessEventConfigurationService = new ExternalBusinessEventConfigurationServiceImpl(eventConfigurationRepository);
    }

    @Override
    public boolean isExternalEventConfiguredForPosting(final BusinessEvent<?> businessEvent) {
        final String eventType = businessEvent.getType();
        final List<MigrationStatus> migrationStatuses = migrationService.findStatuses(businessEvent.getAggregateRootId());
        if (migrationStatuses.contains(MIGRATION_IN_PROGRESS) && !migrationStatuses.contains(MIGRATION_COMPLETED)) {
            return migrationEvents.contains(eventType);
        } else {
            return fallbackExternalBusinessEventConfigurationService.isExternalEventConfiguredForPosting(businessEvent);
        }
    }
}
