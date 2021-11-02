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
package org.apache.fineract.infrastructure.reportmailingjob.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.domain.JdbcSupport;
import org.apache.fineract.infrastructure.core.service.Page;
import org.apache.fineract.infrastructure.core.service.PaginationHelper;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.infrastructure.core.service.SearchParameters;
import org.apache.fineract.infrastructure.reportmailingjob.data.ReportMailingJobRunHistoryData;
import org.apache.fineract.infrastructure.security.utils.ColumnValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class ReportMailingJobRunHistoryReadPlatformServiceImpl implements ReportMailingJobRunHistoryReadPlatformService {

    private final JdbcTemplate jdbcTemplate;
    private final DataSourceSqlResolver sqlResolver;
    private final ReportMailingJobRunHistoryMapper reportMailingJobRunHistoryMapper;
    private final ColumnValidator columnValidator;
    private final PaginationHelper<ReportMailingJobRunHistoryData> paginationHelper = new PaginationHelper<>();

    @Autowired
    public ReportMailingJobRunHistoryReadPlatformServiceImpl(final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver,
                                                             final ColumnValidator columnValidator) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.sqlResolver = sqlResolver;
        this.reportMailingJobRunHistoryMapper = new ReportMailingJobRunHistoryMapper();
        this.columnValidator = columnValidator;
    }

    @Override
    public Page<ReportMailingJobRunHistoryData> retrieveRunHistoryByJobId(final Long reportMailingJobId,
            final SearchParameters searchParameters) {
        final StringBuilder sqlStringBuilder = new StringBuilder(500);
        sqlStringBuilder.append("select ");
        boolean mySql = sqlResolver.getDialect().isMySql();
        if (mySql)
            sqlStringBuilder.append("SQL_CALC_FOUND_ROWS ");

        final List<Object> queryParameters = new ArrayList<>();
        sqlStringBuilder.append(this.reportMailingJobRunHistoryMapper.ReportMailingJobRunHistorySchema());
        String where = "";

        if (reportMailingJobId != null) {
            where = " where rmjrh.job_id = ? ";
            sqlStringBuilder.append(where);
            queryParameters.add(reportMailingJobId);
        }

        if (searchParameters.isOrderByRequested()) {
            sqlStringBuilder.append(" order by ").append(searchParameters.getOrderBy());
            this.columnValidator.validateSqlInjection(sqlStringBuilder.toString(), searchParameters.getOrderBy());
            if (searchParameters.isSortOrderProvided()) {
                sqlStringBuilder.append(" ").append(searchParameters.getSortOrder());
                this.columnValidator.validateSqlInjection(sqlStringBuilder.toString(), searchParameters.getSortOrder());
            }
        }

        if (searchParameters.isLimited()) {
            sqlStringBuilder.append(" limit ").append(searchParameters.getLimit());

            if (searchParameters.isOffset()) {
                sqlStringBuilder.append(" offset ").append(searchParameters.getOffset());
            }
        }

        final Object[] sqlParams = queryParameters.toArray();
        String sqlCountRows = "SELECT FOUND_ROWS()";
        Object[] countParams = null;
        if (!mySql) {
            sqlCountRows = "SELECT " + reportMailingJobRunHistoryMapper.countSchema() + where;
            countParams = sqlParams;
        }
        return this.paginationHelper.fetchPage(this.jdbcTemplate, sqlCountRows, countParams, sqlStringBuilder.toString(), sqlParams, this.reportMailingJobRunHistoryMapper);
    }

    private static final class ReportMailingJobRunHistoryMapper implements RowMapper<ReportMailingJobRunHistoryData> {

        public static final String FROM = " from m_report_mailing_job_run_history rmjrh ";

        public String ReportMailingJobRunHistorySchema() {
            return "rmjrh.id, rmjrh.job_id as report_mailing_job_id, rmjrh.start_datetime as start_date_time, "
                    + "rmjrh.end_datetime as end_date_time, rmjrh.status, rmjrh.error_message as error_message, "
                    + "rmjrh.error_log as error_log "
                    + FROM;
        }

        public String countSchema() {
            return " count(rmjrh.*) " + FROM;
        }

        @Override
        public ReportMailingJobRunHistoryData mapRow(ResultSet rs, int rowNum) throws SQLException {
            final Long id = JdbcSupport.getLong(rs, "id");
            final Long reportMailingJobId = JdbcSupport.getLong(rs, "report_mailing_job_id");
            final ZonedDateTime startDateTime = JdbcSupport.getDateTime(rs, "start_date_time");
            final ZonedDateTime endDateTime = JdbcSupport.getDateTime(rs, "end_date_time");
            final String status = rs.getString("status");
            final String errorMessage = rs.getString("error_message");
            final String errorLog = rs.getString("error_log");

            return ReportMailingJobRunHistoryData.newInstance(id, reportMailingJobId, startDateTime, endDateTime, status,
                    errorMessage, errorLog);
        }
    }
}
