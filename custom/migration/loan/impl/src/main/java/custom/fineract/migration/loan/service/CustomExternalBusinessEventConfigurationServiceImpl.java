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

import custom.fineract.migration.loan.enums.MigrationStatus;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.dataqueries.service.ReadWriteNonCoreDataService;
import org.apache.fineract.infrastructure.event.business.domain.BusinessEvent;
import org.apache.fineract.infrastructure.event.business.service.ExternalBusinessEventConfigurationService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CustomExternalBusinessEventConfigurationServiceImpl implements ExternalBusinessEventConfigurationService {

    private final ReadWriteNonCoreDataService readWriteNonCoreDataService;

    @Value("${custom.migration.loan.migration.events}")
    private List<String> migrationEvents;

    @Override
    public boolean isExternalEventConfiguredForPosting(final BusinessEvent<?> businessEvent) {
        final String eventType = businessEvent.getType();

        if (!doesDtMigrationTableExist()) {
            return true;
        }

        final List<MigrationStatus> migrationStatuses = getMigrationStatuses();

        if (migrationStatuses.contains(MigrationStatus.MIGRATION_COMPLETED)) {
            return true;
        }

        if (migrationStatuses.contains(MigrationStatus.MIGRATION_IN_PROGRESS)
                && !migrationStatuses.contains(MigrationStatus.MIGRATION_COMPLETED)) {
            return migrationEvents.contains(eventType);
        }

        return false;
    }

    private boolean doesDtMigrationTableExist() {
        try {
            return readWriteNonCoreDataService.retrieveDatatable("dt_migration") != null;
        } catch (Exception e) {
            return false;
        }
    }

    private List<MigrationStatus> getMigrationStatuses() {
        return readWriteNonCoreDataService.queryDataTable("dt_migration", "status", null, "status").stream().map(jsonObject -> {
            if (jsonObject.has("status")) {
                try {
                    return MigrationStatus.valueOf(jsonObject.get("status").getAsString());
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

}
