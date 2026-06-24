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

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.core.service.database.DatabaseSpecificSQLGenerator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CustomExternalEventRepositoryImpl implements CustomExternalEventRepository {

    private final JdbcTemplate jdbcTemplate;
    private final DatabaseSpecificSQLGenerator sqlGenerator;

    // Uses DatabaseSpecificSQLGenerator.in() to emit ANY(?) on PostgreSQL instead of IN($1,...$N),
    // so all batch sizes share one query plan instead of one per distinct size.
    @Override
    public void markEventsSent(final List<Long> ids, final OffsetDateTime sentAt) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        final String sql = "UPDATE m_external_event SET status = 'SENT', sent_at = ? WHERE %s".formatted(sqlGenerator.in("id", ids));
        final List<Object> params = new ArrayList<>();
        params.add(Timestamp.from(sentAt.toInstant()));
        Collections.addAll(params, sqlGenerator.inParametersFor(ids));
        jdbcTemplate.update(sql, params.toArray());
    }
}
