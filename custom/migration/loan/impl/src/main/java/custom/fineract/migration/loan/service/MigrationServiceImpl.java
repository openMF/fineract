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

import static custom.fineract.migration.loan.constants.DtMigrationNames.STATUS_COLUMN;

import custom.fineract.migration.loan.constants.DtMigrationNames;
import custom.fineract.migration.loan.enums.MigrationStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.dataqueries.data.GenericResultsetData;
import org.apache.fineract.infrastructure.dataqueries.service.DatatableReadService;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MigrationServiceImpl implements MigrationService {

    private final DatatableReadService datatableReadService;

    @Override
    public Optional<String> findLastStatus(final Long loanId) {
        final GenericResultsetData resultSet = datatableReadService.retrieveDataTableGenericResultSet(DtMigrationNames.DATATABLE_NAME,
                loanId, "id desc", null);
        if (resultSet != null && !resultSet.getData().isEmpty()) {
            final Map<String, Integer> columnMap = determineColumns(resultSet);

            final Object status = resultSet.getData().get(0).getRow().get(columnMap.get(STATUS_COLUMN));
            return Optional.ofNullable((String) status);
        }

        return Optional.empty();
    }

    @Override
    public List<MigrationStatus> findStatuses(final Long loanId) {
        final GenericResultsetData resultSet = datatableReadService.retrieveDataTableGenericResultSet(DtMigrationNames.DATATABLE_NAME,
                loanId, null, null);
        if (resultSet != null && !resultSet.getData().isEmpty()) {
            final Map<String, Integer> columnMap = determineColumns(resultSet);

            return resultSet.getData().stream().map(row -> {
                final Object status = row.getRow().get(columnMap.get(STATUS_COLUMN));
                if (status != null) {
                    try {
                        return MigrationStatus.valueOf(status.toString());
                    } catch (IllegalArgumentException e) {
                        return null;
                    }
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }

        return new ArrayList<>();
    }

    private Map<String, Integer> determineColumns(final GenericResultsetData resultSet) {
        return IntStream.range(0, resultSet.getColumnHeaders().size()).boxed()
                .collect(Collectors.toMap(i -> resultSet.getColumnHeaders().get(i).getColumnName(), i -> i));
    }

}
