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
package org.apache.fineract.cob.savings;

import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.fineract.cob.domain.SavingsAccountLockRepository;
import org.apache.fineract.infrastructure.businessdate.domain.BusinessDateType;
import org.apache.fineract.infrastructure.core.config.FineractProperties;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class SavingsLockingServiceImpl implements SavingsLockingService {

    private static final String BATCH_SAVINGS_LOCK_INSERT = """
                INSERT INTO m_savings_account_locks (savings_id, version, lock_owner, lock_placed_on, lock_placed_on_cob_business_date) VALUES (?,?,?,?,?)
            """;

    private final JdbcTemplate jdbcTemplate;
    private final FineractProperties fineractProperties;
    private final SavingsAccountLockRepository savingsAccountLockRepository;

    @Override
    public void upgradeLock(List<Long> accountsToLock, SavingsLockOwner lockOwner) {
        jdbcTemplate.batchUpdate("""
                    UPDATE m_savings_account_locks SET version= version + 1, lock_owner = ?, lock_placed_on = ? WHERE savings_id = ?
                """, accountsToLock, getInClauseParameterSizeLimit(), (ps, id) -> {
            ps.setString(1, lockOwner.name());
            ps.setObject(2, DateUtils.getAuditOffsetDateTime());
            ps.setLong(3, id);
        });
    }

    @Override
    public List<SavingsAccountLock> findAllBySavingsIdIn(List<Long> savingsIds) {
        return savingsAccountLockRepository.findAllBySavingsIdIn(savingsIds);
    }

    @Override
    public SavingsAccountLock findBySavingsIdAndLockOwner(Long savingsId, SavingsLockOwner lockOwner) {
        return savingsAccountLockRepository.findBySavingsIdAndLockOwner(savingsId, lockOwner).orElseGet(() -> {
            log.warn("There is no lock for savings account with id: {}", savingsId);
            return null;
        });
    }

    @Override
    public List<SavingsAccountLock> findAllBySavingsIdInAndLockOwner(List<Long> savingsIds, SavingsLockOwner lockOwner) {
        return savingsAccountLockRepository.findAllBySavingsIdInAndLockOwner(savingsIds, lockOwner);
    }

    @Override
    public void applyLock(List<Long> savingsIds, SavingsLockOwner lockOwner) {
        LocalDate cobBusinessDate = ThreadLocalContextUtil.getBusinessDateByType(BusinessDateType.COB_DATE);
        jdbcTemplate.batchUpdate(BATCH_SAVINGS_LOCK_INSERT, savingsIds, savingsIds.size(),
                (PreparedStatement ps, Long savingsId) -> {
                    ps.setLong(1, savingsId);
                    ps.setLong(2, 1);
                    ps.setString(3, lockOwner.name());
                    ps.setObject(4, DateUtils.getAuditOffsetDateTime());
                    ps.setObject(5, cobBusinessDate);
                });
    }

    @Override
    public void deleteBySavingsIdInAndLockOwner(List<Long> savingsIds, SavingsLockOwner lockOwner) {
        savingsAccountLockRepository.deleteBySavingsIdInAndLockOwner(savingsIds, lockOwner);
    }

    private int getInClauseParameterSizeLimit() {
        return fineractProperties.getQuery().getInClauseParameterSizeLimit();
    }
}
