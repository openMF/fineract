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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.cob.data.AccountCOBParameter;
import org.apache.fineract.cob.data.AccountCOBPartition;
import org.apache.fineract.cob.data.AccountIdAndExternalIdAndAccountNo;
import org.apache.fineract.cob.data.AccountIdAndLastClosedBusinessDate;
import org.apache.fineract.cob.savings.RetrieveSavingsIdService;
import org.apache.fineract.cob.savings.SavingsCOBConstant;
import org.apache.fineract.infrastructure.businessdate.domain.BusinessDateType;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.portfolio.savings.domain.SavingsAccountRepository;
import org.apache.fineract.portfolio.savings.domain.SavingsAccountStatusType;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@RequiredArgsConstructor
public class RetrieveAllNonClosedSavingsIdServiceImpl implements RetrieveSavingsIdService {

    private static final Collection<Integer> NON_CLOSED_SAVINGS_STATUSES = new ArrayList<>(
            Arrays.asList(SavingsAccountStatusType.SUBMITTED_AND_PENDING_APPROVAL.getValue(), SavingsAccountStatusType.APPROVED.getValue(), SavingsAccountStatusType.ACTIVE.getValue(),
                    SavingsAccountStatusType.TRANSFER_IN_PROGRESS.getValue(), SavingsAccountStatusType.TRANSFER_ON_HOLD.getValue()));

    private final SavingsAccountRepository savingsRepository;

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Override
    public List<AccountCOBPartition> retrieveSavingsCOBPartitions(Long numberOfDays, LocalDate businessDate, boolean isCatchUp,
            int partitionSize) {
        StringBuilder sql = new StringBuilder();
        sql.append("select min(id) as min, max(id) as max, page, count(id) as count from ");
        sql.append("  (select floor(((row_number() over(order by id))-1) / :pageSize) as page, t.* from ");
        sql.append("      (select id from m_savings_account where status_enum in (:statusIds) and ");
        if (isCatchUp) {
            sql.append("last_closed_business_date = :businessDate ");
        } else {
            sql.append("(last_closed_business_date = :businessDate or last_closed_business_date is null) ");
        }
        sql.append("order by id) t) t2 ");
        sql.append("group by page ");
        sql.append("order by page");

        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("pageSize", partitionSize);
        parameters.addValue("statusIds", List.of(100, 200, 300, 303, 304));
        parameters.addValue("businessDate", businessDate.minusDays(numberOfDays));
        return namedParameterJdbcTemplate.query(sql.toString(), parameters, RetrieveAllNonClosedSavingsIdServiceImpl::mapRow);
    }

    private static AccountCOBPartition mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new AccountCOBPartition(rs.getLong("min"), rs.getLong("max"), rs.getLong("page"), rs.getLong("count"));
    }

    @Override
    public List<AccountIdAndLastClosedBusinessDate> retrieveSavingsIdsBehindDate(LocalDate businessDate, List<Long> savingsIds) {
        return savingsRepository.findAllSavingsBehindBySavingsIdsAndStatuses(businessDate, savingsIds, NON_CLOSED_SAVINGS_STATUSES);
    }

    @Override
    public List<AccountIdAndLastClosedBusinessDate> retrieveSavingsIdsBehindDateOrNull(LocalDate businessDate, List<Long> savingsIds) {
        return savingsRepository.findAllSavingsBehindOrNullBySavingsIdsAndStatuses(businessDate, savingsIds, NON_CLOSED_SAVINGS_STATUSES);
    }

    @Override
    public List<AccountIdAndLastClosedBusinessDate> retrieveSavingsIdsOldestCobProcessed(LocalDate businessDate) {
        return savingsRepository.findOldestCOBProcessedSavings(businessDate, NON_CLOSED_SAVINGS_STATUSES);
    }

    @Override
    public List<Long> retrieveAllNonClosedSavingsByLastClosedBusinessDateAndMinAndMaxSavingsId(AccountCOBParameter savingsCOBParameter,
            boolean isCatchUp) {
        if (isCatchUp) {
            return savingsRepository.findAllSavingsByLastClosedBusinessDateNotNullAndMinAndMaxSavingsIdAndStatuses(
                    savingsCOBParameter.getMinAccountId(), savingsCOBParameter.getMaxAccountId(), ThreadLocalContextUtil
                            .getBusinessDateByType(BusinessDateType.COB_DATE).minusDays(SavingsCOBConstant.NUMBER_OF_DAYS_BEHIND),
                    NON_CLOSED_SAVINGS_STATUSES);
        } else {
            return savingsRepository.findAllSavingsByLastClosedBusinessDateAndMinAndMaxSavingsIdAndStatuses(
                    savingsCOBParameter.getMinAccountId(), savingsCOBParameter.getMaxAccountId(), ThreadLocalContextUtil
                            .getBusinessDateByType(BusinessDateType.COB_DATE).minusDays(SavingsCOBConstant.NUMBER_OF_DAYS_BEHIND),
                    NON_CLOSED_SAVINGS_STATUSES);
        }
    }

    @Override
    public List<AccountIdAndExternalIdAndAccountNo> findAllStayedLockedByCobBusinessDate(LocalDate cobBusinessDate) {
        return savingsRepository.findAllStayedLockedByCobBusinessDate(cobBusinessDate);
    }

}
