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
package org.apache.fineract.cob.domain;

import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.infrastructure.businessdate.domain.BusinessDateType;
import org.apache.fineract.infrastructure.core.config.FineractProperties;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Slf4j
public abstract class AbstractLockingService implements LockingService {

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final FineractProperties fineractProperties;

    protected AbstractLockingService(JdbcTemplate jdbcTemplate, FineractProperties fineractProperties) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        this.fineractProperties = fineractProperties;
    }

    protected abstract String getTableName();

    protected abstract String getBatchLoanLockInsert();

    protected abstract String getBatchLoanLockUpgrade();

    @Override
    public void upgradeLock(List<Long> accountsToLock, LockOwner lockOwner) {
        jdbcTemplate.batchUpdate(getBatchLoanLockUpgrade(), accountsToLock, getInClauseParameterSizeLimit(), (ps, id) -> {
            ps.setString(1, lockOwner.name());
            ps.setObject(2, DateUtils.getAuditOffsetDateTime());
            ps.setLong(3, id);
        });
    }

    @Override
    public List<Long> findLockIdsByLoanIdIn(List<Long> loanIds) {
        if (loanIds.isEmpty()) {
            return Collections.emptyList();
        }
        String sql = "SELECT loan_id FROM " + getTableName() + " WHERE loan_id IN (:ids)";
        return namedParameterJdbcTemplate.queryForList(sql, Map.of("ids", loanIds), Long.class);
    }

    @Override
    public List<Long> findLockIdsByLoanIdInAndLockOwner(List<Long> loanIds, LockOwner lockOwner) {
        if (loanIds.isEmpty()) {
            return Collections.emptyList();
        }
        String sql = "SELECT loan_id FROM " + getTableName() + " WHERE loan_id IN (:ids) AND lock_owner = :owner";
        return namedParameterJdbcTemplate.queryForList(sql, Map.of("ids", loanIds, "owner", lockOwner.name()), Long.class);
    }

    @Override
    public void applyLock(List<Long> loanIds, LockOwner lockOwner) {
        LocalDate cobBusinessDate = ThreadLocalContextUtil.getBusinessDateByType(BusinessDateType.COB_DATE);
        jdbcTemplate.batchUpdate(getBatchLoanLockInsert(), loanIds, loanIds.size(), (PreparedStatement ps, Long loanId) -> {
            ps.setLong(1, loanId);
            ps.setLong(2, 1);
            ps.setString(3, lockOwner.name());
            ps.setObject(4, DateUtils.getAuditOffsetDateTime());
            ps.setObject(5, cobBusinessDate);
        });
    }

    @Override
    public void deleteByLoanIdInAndLockOwner(List<Long> loanIds, LockOwner lockOwner) {
        if (loanIds.isEmpty()) {
            return;
        }
        String sql = "DELETE FROM " + getTableName() + " WHERE loan_id IN (:ids) AND lock_owner = :owner";
        namedParameterJdbcTemplate.update(sql, Map.of("ids", loanIds, "owner", lockOwner.name()));
    }

    @Override
    public void updateLockError(Long loanId, LockOwner lockOwner, String error, String stacktrace) {
        String sql = "UPDATE " + getTableName() + " SET error = ?, stacktrace = ? WHERE loan_id = ? AND lock_owner = ?";
        int updated = jdbcTemplate.update(sql, error, stacktrace, loanId, lockOwner.name());
        if (updated == 0) {
            log.warn("No lock found to update error for loan id: {} with owner: {}", loanId, lockOwner);
        }
    }

    private int getInClauseParameterSizeLimit() {
        return fineractProperties.getQuery().getInClauseParameterSizeLimit();
    }
}
