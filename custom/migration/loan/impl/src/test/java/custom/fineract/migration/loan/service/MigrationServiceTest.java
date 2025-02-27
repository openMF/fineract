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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.fineract.infrastructure.core.service.database.DatabaseType;
import org.apache.fineract.infrastructure.dataqueries.data.GenericResultsetData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetRowData;
import org.apache.fineract.infrastructure.dataqueries.service.ReadWriteNonCoreDataService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MigrationServiceTest {

    @Mock
    private ReadWriteNonCoreDataService readWriteNonCoreDataService;

    @InjectMocks
    private MigrationServiceImpl migrationService;

    private static final Long TEST_LOAN_ID = 1L;
    private static final String TEST_STATUS = "MIGRATION_COMPLETED";

    @Test
    void findLastStatus_WhenDataExists_ShouldReturnStatus() {
        final List<ResultsetColumnHeaderData> columnHeaders = Arrays.asList(
                ResultsetColumnHeaderData.basic("id", "INT", DatabaseType.MYSQL),
                ResultsetColumnHeaderData.basic("status", "VARCHAR", DatabaseType.MYSQL),
                ResultsetColumnHeaderData.basic("loan_id", "INT", DatabaseType.MYSQL));

        final ResultsetRowData rowData = ResultsetRowData.create(Arrays.asList(2, TEST_STATUS, 1, 1, "SOME_STATUS", 1));
        final GenericResultsetData resultSet = new GenericResultsetData(columnHeaders, Arrays.asList(rowData));

        when(readWriteNonCoreDataService.retrieveDataTableGenericResultSet(eq("dt_migration"), eq(TEST_LOAN_ID), eq("id desc"), any()))
                .thenReturn(resultSet);

        final Optional<String> result = migrationService.findLastStatus(TEST_LOAN_ID);

        assertTrue(result.isPresent());
        assertEquals(TEST_STATUS, result.get());
    }

    @Test
    void findLastStatus_WhenNoDataExists_ShouldReturnEmpty() {
        final List<ResultsetColumnHeaderData> columnHeaders = Arrays.asList(
                ResultsetColumnHeaderData.basic("id", "INT", DatabaseType.MYSQL),
                ResultsetColumnHeaderData.basic("status", "VARCHAR", DatabaseType.MYSQL));

        final GenericResultsetData resultSet = new GenericResultsetData(columnHeaders, new ArrayList<>());

        when(readWriteNonCoreDataService.retrieveDataTableGenericResultSet(eq("dt_migration"), eq(TEST_LOAN_ID), eq("id desc"), any()))
                .thenReturn(resultSet);

        final Optional<String> result = migrationService.findLastStatus(TEST_LOAN_ID);

        assertFalse(result.isPresent());
    }

    @Test
    void findLastStatus_WhenResultSetIsNull_ShouldReturnEmpty() {
        when(readWriteNonCoreDataService.retrieveDataTableGenericResultSet(
            eq("dt_migration"), eq(TEST_LOAN_ID), eq("id desc"), any()))
            .thenReturn(null);

        final Optional<String> result = migrationService.findLastStatus(TEST_LOAN_ID);

        assertFalse(result.isPresent());
    }

    @Test
    void findLastStatus_WhenStatusIsNull_ShouldReturnEmpty() {
        final List<ResultsetColumnHeaderData> columnHeaders = Arrays.asList(
                ResultsetColumnHeaderData.basic("id", "INT", DatabaseType.MYSQL),
                ResultsetColumnHeaderData.basic("status", "VARCHAR", DatabaseType.MYSQL),
                ResultsetColumnHeaderData.basic("loan_id", "INT", DatabaseType.MYSQL));

        final ResultsetRowData rowData = ResultsetRowData.create(Arrays.asList(2, null, 1));
        final GenericResultsetData resultSet = new GenericResultsetData(columnHeaders, Arrays.asList(rowData));

        when(readWriteNonCoreDataService.retrieveDataTableGenericResultSet(eq("dt_migration"), eq(TEST_LOAN_ID), eq("id desc"), any()))
                .thenReturn(resultSet);

        final Optional<String> result = migrationService.findLastStatus(TEST_LOAN_ID);

        assertFalse(result.isPresent());
    }

}
