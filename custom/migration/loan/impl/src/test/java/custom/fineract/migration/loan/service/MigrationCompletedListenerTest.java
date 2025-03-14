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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import custom.fineract.migration.loan.constants.DtMigrationNames;
import custom.fineract.migration.loan.enums.MigrationStatus;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.fineract.infrastructure.dataqueries.data.EntityTables;
import org.apache.fineract.infrastructure.event.business.domain.datatable.DatatableEntryCreatedBusinessEvent;
import org.apache.fineract.infrastructure.event.business.domain.datatable.DatatableEntryDetails;
import org.apache.fineract.infrastructure.event.business.domain.loan.LoanAccountSnapshotBusinessEvent;
import org.apache.fineract.infrastructure.event.business.service.BusinessEventNotifierService;
import org.apache.fineract.portfolio.loanaccount.domain.Loan;
import org.apache.fineract.portfolio.loanaccount.domain.LoanRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MigrationCompletedListenerTest {

    @Mock
    private BusinessEventNotifierService businessEventNotifierService;

    @Mock
    private LoanRepository loanRepository;

    @Mock
    private DatatableEntryCreatedBusinessEvent event;

    @Mock
    private Loan loan;

    private MigrationCompletedListener listener;

    @BeforeEach
    void setUp() {
        listener = new MigrationCompletedListener(businessEventNotifierService, loanRepository);
    }

    @Test
    void shouldProcessEventWhenMigrationIsCompleted() {
        final Long loanId = 1L;
        final Map<String, Object> data = new HashMap<>();
        data.put(DtMigrationNames.STATUS_COLUMN, MigrationStatus.MIGRATION_COMPLETED.toString());

        when(event.getDatatableEntryDetails()).thenReturn(mockDatatableEntryDetails(DtMigrationNames.DATATABLE_NAME, loanId, data));
        when(loanRepository.findById(loanId)).thenReturn(Optional.of(loan));

        listener.onBusinessEvent(event);

        final ArgumentCaptor<LoanAccountSnapshotBusinessEvent> eventCaptor = ArgumentCaptor
                .forClass(LoanAccountSnapshotBusinessEvent.class);
        verify(businessEventNotifierService).notifyPostBusinessEvent(eventCaptor.capture());

        assertThat(eventCaptor.getValue().get()).isEqualTo(loan);
    }

    @Test
    void shouldNotProcessEventWhenDatatableNameIsDifferent() {
        when(event.getDatatableEntryDetails()).thenReturn(mockDatatableEntryDetails("different_table", 1L, new HashMap<>()));

        listener.onBusinessEvent(event);

        verifyNoInteractions(loanRepository, businessEventNotifierService);
    }

    @Test
    void shouldNotProcessEventWhenStatusIsNotCompleted() {
        final Map<String, Object> data = new HashMap<>();
        data.put(DtMigrationNames.STATUS_COLUMN, "IN_PROGRESS");

        when(event.getDatatableEntryDetails()).thenReturn(mockDatatableEntryDetails(DtMigrationNames.DATATABLE_NAME, 1L, data));

        listener.onBusinessEvent(event);

        verifyNoInteractions(loanRepository, businessEventNotifierService);
    }

    @Test
    void shouldNotProcessEventWhenStatusIsNull() {
        final Map<String, Object> data = new HashMap<>();
        data.put(DtMigrationNames.STATUS_COLUMN, null);

        when(event.getDatatableEntryDetails()).thenReturn(mockDatatableEntryDetails(DtMigrationNames.DATATABLE_NAME, 1L, data));

        listener.onBusinessEvent(event);

        verifyNoInteractions(loanRepository, businessEventNotifierService);
    }

    @Test
    void shouldNotProcessEventWhenLoanNotFound() {
        final Long loanId = 1L;
        final Map<String, Object> data = new HashMap<>();
        data.put(DtMigrationNames.STATUS_COLUMN, MigrationStatus.MIGRATION_COMPLETED.toString());

        when(event.getDatatableEntryDetails()).thenReturn(mockDatatableEntryDetails(DtMigrationNames.DATATABLE_NAME, loanId, data));
        when(loanRepository.findById(loanId)).thenReturn(Optional.empty());

        listener.onBusinessEvent(event);

        verifyNoInteractions(businessEventNotifierService);
    }

    private DatatableEntryDetails mockDatatableEntryDetails(final String datatableName, final Long entityId,
            final Map<String, Object> data) {
        return new DatatableEntryDetails(datatableName, EntityTables.LOAN, entityId, entityId, data);
    }

}
