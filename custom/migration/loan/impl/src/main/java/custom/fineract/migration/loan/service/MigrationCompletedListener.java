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

import custom.fineract.migration.loan.constants.DtMigrationNames;
import custom.fineract.migration.loan.enums.MigrationStatus;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.event.business.BusinessEventListener;
import org.apache.fineract.infrastructure.event.business.domain.datatable.DatatableEntryCreatedBusinessEvent;
import org.apache.fineract.infrastructure.event.business.domain.loan.LoanAccountSnapshotBusinessEvent;
import org.apache.fineract.infrastructure.event.business.service.BusinessEventNotifierService;
import org.apache.fineract.portfolio.loanaccount.domain.LoanRepository;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MigrationCompletedListener implements BusinessEventListener<DatatableEntryCreatedBusinessEvent> {

    private final BusinessEventNotifierService businessEventNotifierService;
    private final LoanRepository loanRepository;

    @Override
    public void onBusinessEvent(final DatatableEntryCreatedBusinessEvent event) {
        if (!DtMigrationNames.DATATABLE_NAME.equals(event.getDatatableEntryDetails().getDatatableName())) {
            return;
        }

        final String status = (String) event.getDatatableEntryDetails().getData().get(DtMigrationNames.STATUS_COLUMN);
        if (status == null || !MigrationStatus.MIGRATION_COMPLETED.toString().equals(status)) {
            return;
        }

        final Long loanId = event.getDatatableEntryDetails().getEntityId();

        loanRepository.findById(loanId)
                .ifPresent(loan -> businessEventNotifierService.notifyPostBusinessEvent(new LoanAccountSnapshotBusinessEvent(loan)));
    }

}
