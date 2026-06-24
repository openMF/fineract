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
package org.apache.fineract.infrastructure.event.external.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.fineract.infrastructure.core.service.database.DatabaseSpecificSQLGenerator;
import org.apache.fineract.infrastructure.core.service.database.DatabaseType;
import org.apache.fineract.infrastructure.core.service.database.DatabaseTypeResolver;
import org.apache.fineract.infrastructure.core.service.database.RoutingDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

@ExtendWith(MockitoExtension.class)
class CustomExternalEventRepositoryImplTest {

    private static final List<Long> EVENT_IDS = List.of(1L, 2L, 3L);
    private static final OffsetDateTime SENT_AT = OffsetDateTime.of(2026, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC);

    @Mock
    private JdbcTemplate jdbcTemplate;
    @Mock
    private DatabaseTypeResolver databaseTypeResolver;
    @Mock
    private RoutingDataSource dataSource;

    private CustomExternalEventRepositoryImpl underTest;

    @BeforeEach
    void setUp() {
        final DatabaseSpecificSQLGenerator sqlGenerator = new DatabaseSpecificSQLGenerator(databaseTypeResolver, dataSource);
        underTest = new CustomExternalEventRepositoryImpl(jdbcTemplate, sqlGenerator);
    }

    @Test
    void markEventsSentOnMySqlUsesInClauseWithIndividualBindParameters() {
        when(databaseTypeResolver.databaseType()).thenReturn(DatabaseType.MYSQL);

        underTest.markEventsSent(EVENT_IDS, SENT_AT);

        final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(Timestamp.from(SENT_AT.toInstant())), eq(1L), eq(2L), eq(3L));
        assertEquals("UPDATE m_external_event SET status = 'SENT', sent_at = ? WHERE id IN (?,?,?)", sqlCaptor.getValue());
    }

    @Test
    void markEventsSentOnPostgresUsesAnyClauseWithArrayBindParameter() throws SQLException {
        when(databaseTypeResolver.databaseType()).thenReturn(DatabaseType.POSTGRESQL);
        final Connection connection = mock(Connection.class);
        final Array idArray = mock(Array.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createArrayOf(eq("bigint"), any(Long[].class))).thenReturn(idArray);

        underTest.markEventsSent(EVENT_IDS, SENT_AT);

        final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(Timestamp.from(SENT_AT.toInstant())), eq(idArray));
        assertEquals("UPDATE m_external_event SET status = 'SENT', sent_at = ? WHERE id = ANY (?)", sqlCaptor.getValue());
        verify(connection).createArrayOf("bigint", EVENT_IDS.toArray(Long[]::new));
    }

    @ParameterizedTest
    @MethodSource("nullOrEmptyEventIds")
    void markEventsSentWithNoIdsSkipsUpdate(final List<Long> eventIds) {
        underTest.markEventsSent(eventIds, SENT_AT);

        verifyNoInteractions(jdbcTemplate);
    }

    private static Stream<List<Long>> nullOrEmptyEventIds() {
        return Stream.of(null, Collections.emptyList());
    }
}
