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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import custom.fineract.migration.loan.enums.MigrationStatus;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import org.apache.fineract.infrastructure.dataqueries.data.DatatableData;
import org.apache.fineract.infrastructure.dataqueries.service.DatatableReadService;
import org.apache.fineract.infrastructure.event.business.domain.BusinessEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CustomExternalBusinessEventConfigurationServiceTest {

    @Mock
    private DatatableReadService datatableReadService;

    @Mock
    private MigrationService migrationService;

    private CustomExternalBusinessEventConfigurationServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        final List<String> mockMigrationEvents = List.of("LoanCreatedBusinessEvent", "LoanDisbursalBusinessEvent",
                "LoanTransactionMakeRepaymentPostBusinessEvent");
        service = new CustomExternalBusinessEventConfigurationServiceImpl(migrationService, datatableReadService);
        final Field field = CustomExternalBusinessEventConfigurationServiceImpl.class.getDeclaredField("migrationEvents");
        field.setAccessible(true);
        field.set(service, mockMigrationEvents);
    }

    @Test
    void testIsExternalEventConfiguredForPosting_NoDtMigrationTable() {
        when(datatableReadService.retrieveDatatable("dt_migration")).thenThrow(new RuntimeException());

        final BusinessEvent<?> event = mock(BusinessEvent.class);
        when(event.getType()).thenReturn("LoanCreatedBusinessEvent");

        assertTrue(service.isExternalEventConfiguredForPosting(event));
    }

    @Test
    void testIsExternalEventConfiguredForPosting_MigrationCompleted() {
        when(datatableReadService.retrieveDatatable("dt_migration")).thenReturn(mock(DatatableData.class));
        final BusinessEvent<?> event = mock(BusinessEvent.class);
        when(event.getType()).thenReturn("LoanCreatedBusinessEvent");
        when(event.getAggregateRootId()).thenReturn(1L);

        when(migrationService.findStatuses(1L)).thenReturn(Collections.singletonList(MigrationStatus.MIGRATION_COMPLETED));

        assertTrue(service.isExternalEventConfiguredForPosting(event));
    }

    @Test
    void testIsExternalEventConfiguredForPosting_MigrationInProgressAndEventInList() {
        when(datatableReadService.retrieveDatatable("dt_migration")).thenReturn(mock(DatatableData.class));
        final BusinessEvent<?> event = mock(BusinessEvent.class);
        when(event.getType()).thenReturn("LoanCreatedBusinessEvent");
        when(event.getAggregateRootId()).thenReturn(1L);

        when(migrationService.findStatuses(1L)).thenReturn(Collections.singletonList(MigrationStatus.MIGRATION_IN_PROGRESS));

        assertTrue(service.isExternalEventConfiguredForPosting(event));
    }

    @Test
    void testIsExternalEventConfiguredForPosting_MigrationInProgressAndEventNotInList() {
        when(datatableReadService.retrieveDatatable("dt_migration")).thenReturn(mock(DatatableData.class));
        final BusinessEvent<?> event = mock(BusinessEvent.class);
        when(event.getType()).thenReturn("SomeOtherEvent");
        when(event.getAggregateRootId()).thenReturn(1L);

        when(migrationService.findStatuses(1L)).thenReturn(Collections.singletonList(MigrationStatus.MIGRATION_IN_PROGRESS));

        assertFalse(service.isExternalEventConfiguredForPosting(event));
    }

}
