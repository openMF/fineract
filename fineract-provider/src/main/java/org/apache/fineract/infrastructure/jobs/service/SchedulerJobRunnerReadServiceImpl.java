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
package org.apache.fineract.infrastructure.jobs.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.service.Page;
import org.apache.fineract.infrastructure.core.service.PaginationHelper;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.infrastructure.core.service.SearchParameters;
import org.apache.fineract.infrastructure.jobs.data.JobDetailData;
import org.apache.fineract.infrastructure.jobs.data.JobDetailHistoryData;
import org.apache.fineract.infrastructure.jobs.exception.JobNotFoundException;
import org.apache.fineract.infrastructure.jobs.exception.OperationNotAllowedException;
import org.apache.fineract.infrastructure.security.utils.ColumnValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class SchedulerJobRunnerReadServiceImpl implements SchedulerJobRunnerReadService {

    private final JdbcTemplate jdbcTemplate;
    private final DataSourceSqlResolver sqlResolver;
    private final ColumnValidator columnValidator;

    private final PaginationHelper<JobDetailHistoryData> paginationHelper = new PaginationHelper<>();

    @Autowired
    public SchedulerJobRunnerReadServiceImpl(final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver,
                                             final ColumnValidator columnValidator) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.sqlResolver = sqlResolver;
        this.columnValidator = columnValidator;
    }

    @Override
    public List<JobDetailData> findAllJobDeatils() {
        final JobDetailMapper detailMapper = new JobDetailMapper();
        final String sql = detailMapper.schema();
        final List<JobDetailData> JobDeatils = this.jdbcTemplate.query(sql, detailMapper, new Object[] {});
        return JobDeatils;

    }

    @Override
    public JobDetailData retrieveOne(final Long jobId) {
        try {
            final JobDetailMapper detailMapper = new JobDetailMapper();
            final String sql = detailMapper.schema() + " where job.id=?";
            return this.jdbcTemplate.queryForObject(sql, detailMapper, new Object[] { jobId });
        } catch (final EmptyResultDataAccessException e) {
            throw new JobNotFoundException(String.valueOf(jobId), e);
        }
    }

    @Override
    public Page<JobDetailHistoryData> retrieveJobHistory(final Long jobId, final SearchParameters searchParameters) {
        if (!isJobExist(jobId)) {
            throw new JobNotFoundException(String.valueOf(jobId));
        }
        final JobHistoryMapper jobHistoryMapper = new JobHistoryMapper();
        final StringBuilder sqlBuilder = new StringBuilder("select ");
        boolean mySql = sqlResolver.getDialect().isMySql();
        if (mySql)
            sqlBuilder.append("SQL_CALC_FOUND_ROWS ");

        sqlBuilder.append(jobHistoryMapper.schema());
        final String where = " where job.id=?";
        sqlBuilder.append(where);
        if (searchParameters.isOrderByRequested()) {
            sqlBuilder.append(" order by ").append(searchParameters.getOrderBy());
            this.columnValidator.validateSqlInjection(sqlBuilder.toString(), searchParameters.getOrderBy());
            if (searchParameters.isSortOrderProvided()) {
                sqlBuilder.append(' ').append(searchParameters.getSortOrder());
                this.columnValidator.validateSqlInjection(sqlBuilder.toString(), searchParameters.getSortOrder());
            }
        }

        if (searchParameters.isLimited()) {
            sqlBuilder.append(" limit ").append(searchParameters.getLimit());
            if (searchParameters.isOffset()) {
                sqlBuilder.append(" offset ").append(searchParameters.getOffset());
            }
        }

        final Object[] sqlParams = new Object[] {jobId};
        String sqlCountRows = "SELECT FOUND_ROWS()";
        Object[] countParams = null;
        if (!mySql) {
            sqlCountRows = "SELECT " + jobHistoryMapper.countSchema() + where;
            countParams = sqlParams;
        }
        return this.paginationHelper.fetchPage(this.jdbcTemplate, sqlCountRows, countParams, sqlBuilder.toString(), sqlParams, jobHistoryMapper);
    }

    @Override
    public boolean isUpdatesAllowed() {
        final String sql = "select job.display_name from job job where job.currently_running = " + sqlResolver.formatBoolValue(true) +
                " and job.updates_allowed = " + sqlResolver.formatBoolValue(false);
        final List<String> names = this.jdbcTemplate.queryForList(sql, String.class);
        if (names.size() > 0) {
            final String listVals = names.toString();
            final String jobNames = listVals.substring(listVals.indexOf("[") + 1, listVals.indexOf("]"));
            throw new OperationNotAllowedException(jobNames);
        }
        return true;
    }

    private boolean isJobExist(final Long jobId) {
        boolean isJobPresent = false;
        try {
            final String sql = "select count(*) from job job where job.id= ?";
            final int count = this.jdbcTemplate.queryForObject(sql, Integer.class, new Object[] { jobId });
            if (count == 1) {
                isJobPresent = true;
            }
            return isJobPresent;
        } catch (EmptyResultDataAccessException e) {
            return isJobPresent;
        }

    }

    private final class JobDetailMapper implements RowMapper<JobDetailData> {

        private final StringBuilder sqlBuilder = new StringBuilder("select")
                .append(" job.id,job.display_name as display_name,job.next_run_time as next_run_time,job.initializing_errorlog as initializing_error,job.cron_expression as cron_expression,job.is_active as active,job.currently_running as currently_running,")
                .append(" run_history.version,run_history.start_time as last_run_start_time,run_history.end_time as last_run_end_time,run_history." + sqlResolver.toDefinition("status") + ",run_history.error_message as job_run_error_message,run_history.trigger_type as trigger_type,run_history.error_log as job_run_error_log ")
                .append(" from job job  left join job_run_history run_history ON job.id=run_history.job_id and job.previous_run_start_time=run_history.start_time ");

        public String schema() {
            return this.sqlBuilder.toString();
        }

        @Override
        public JobDetailData mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {
            final Long id = rs.getLong("id");
            final String displayName = rs.getString("displayName");
            final Date nextRunTime = rs.getTimestamp("nextRunTime");
            final String initializingError = rs.getString("initializingError");
            final String cronExpression = rs.getString("cronExpression");
            final boolean active = rs.getBoolean("active");
            final boolean currentlyRunning = rs.getBoolean("currentlyRunning");

            final Long version = rs.getLong("version");
            final Date jobRunStartTime = rs.getTimestamp("lastRunStartTime");
            final Date jobRunEndTime = rs.getTimestamp("lastRunEndTime");
            final String status = rs.getString("status");
            final String jobRunErrorMessage = rs.getString("jobRunErrorMessage");
            final String triggerType = rs.getString("triggerType");
            final String jobRunErrorLog = rs.getString("jobRunErrorLog");

            JobDetailHistoryData lastRunHistory = null;
            if (version > 0) {
                lastRunHistory = new JobDetailHistoryData(version, jobRunStartTime, jobRunEndTime, status, jobRunErrorMessage, triggerType,
                        jobRunErrorLog);
            }
            final JobDetailData jobDetail = new JobDetailData(id, displayName, nextRunTime, initializingError, cronExpression, active,
                    currentlyRunning, lastRunHistory);
            return jobDetail;
        }

    }

    private final class JobHistoryMapper implements RowMapper<JobDetailHistoryData> {

        public static final String FROM = " from job job join job_run_history run_history ON job.id=run_history.job_id ";

        private final StringBuilder sqlBuilder = new StringBuilder(200)
                .append(" run_history.version,run_history.start_time as run_start_time,run_history.end_time as run_end_time,run_history." + sqlResolver.toDefinition("status") + ",run_history.error_message as job_run_error_message,run_history.trigger_type as trigger_type,run_history.error_log as job_run_error_log ")
                .append(FROM);

        public String schema() {
            return this.sqlBuilder.toString();
        }

        public String countSchema() {
            return " count(job.*) " + FROM;
        }

        @Override
        public JobDetailHistoryData mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {
            final Long version = rs.getLong("version");
            final Date jobRunStartTime = rs.getTimestamp("run_start_time");
            final Date jobRunEndTime = rs.getTimestamp("run_end_time");
            final String status = rs.getString("status");
            final String jobRunErrorMessage = rs.getString("job_run_error_message");
            final String triggerType = rs.getString("trigger_type");
            final String jobRunErrorLog = rs.getString("job_run_error_log");
            final JobDetailHistoryData jobDetailHistory = new JobDetailHistoryData(version, jobRunStartTime, jobRunEndTime, status,
                    jobRunErrorMessage, triggerType, jobRunErrorLog);
            return jobDetailHistory;
        }
    }
}
