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
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.data.EnumOptionData;
import org.apache.fineract.infrastructure.core.domain.JdbcSupport;
import org.apache.fineract.infrastructure.core.service.Page;
import org.apache.fineract.infrastructure.core.service.PaginationHelper;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.infrastructure.core.service.SearchParameters;
import org.apache.fineract.infrastructure.dataqueries.data.ReportData;
import org.apache.fineract.infrastructure.reportmailingjob.data.ReportMailingJobData;
import org.apache.fineract.infrastructure.reportmailingjob.data.ReportMailingJobEmailAttachmentFileFormat;
import org.apache.fineract.infrastructure.reportmailingjob.data.ReportMailingJobStretchyReportParamDateOption;
import org.apache.fineract.infrastructure.reportmailingjob.data.ReportMailingJobTimelineData;
import org.apache.fineract.infrastructure.reportmailingjob.exception.ReportMailingJobNotFoundException;
import org.apache.fineract.infrastructure.security.utils.ColumnValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class ReportMailingJobReadPlatformServiceImpl implements ReportMailingJobReadPlatformService {

    private final JdbcTemplate jdbcTemplate;
    private final DataSourceSqlResolver sqlResolver;
    private final ColumnValidator columnValidator;

    @Autowired
    public ReportMailingJobReadPlatformServiceImpl(final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver,
                                                   final ColumnValidator columnValidator) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.sqlResolver = sqlResolver;
        this.columnValidator = columnValidator;
    }

    @Override
    public Page<ReportMailingJobData> retrieveAllReportMailingJobs(final SearchParameters searchParameters) {
        final List<Object> queryParameters = new ArrayList<>();
        final ReportMailingJobMapper mapper = new ReportMailingJobMapper();
        final PaginationHelper<ReportMailingJobData> paginationHelper = new PaginationHelper<>();

        final StringBuilder sqlStringBuilder = new StringBuilder("select ");
        boolean mySql = sqlResolver.getDialect().isMySql();
        if (mySql)
            sqlStringBuilder.append("SQL_CALC_FOUND_ROWS ");

        sqlStringBuilder.append(mapper.ReportMailingJobSchema());
        final String where = " where rmj.is_deleted = " + sqlResolver.formatBoolValue(false);
        sqlStringBuilder.append(where);

        if (searchParameters.isOrderByRequested()) {
            sqlStringBuilder.append(" order by ").append(searchParameters.getOrderBy());
            this.columnValidator.validateSqlInjection(sqlStringBuilder.toString(), searchParameters.getOrderBy());
            if (searchParameters.isSortOrderProvided()) {
                sqlStringBuilder.append(" ").append(searchParameters.getSortOrder());
                this.columnValidator.validateSqlInjection(sqlStringBuilder.toString(), searchParameters.getSortOrder());
            }
        } else {
            sqlStringBuilder.append(" order by rmj.name ");
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
            sqlCountRows = "SELECT " + mapper.countSchema() + where;
            countParams = sqlParams;
        }
        return paginationHelper.fetchPage(this.jdbcTemplate, sqlCountRows, countParams, sqlStringBuilder.toString(), sqlParams, mapper);
    }

    @Override
    public Collection<ReportMailingJobData> retrieveAllActiveReportMailingJobs() {
        final ReportMailingJobMapper mapper = new ReportMailingJobMapper();
        final String sql = "select " + mapper.ReportMailingJobSchema() + " where rmj.is_deleted = "
                + sqlResolver.formatBoolValue(false)
                + " and is_active = "
                + sqlResolver.formatBoolValue(true)
                + " order by rmj.name";

        return this.jdbcTemplate.query(sql, mapper, new Object[] {});
    }

    @Override
    public ReportMailingJobData retrieveReportMailingJob(final Long reportMailingJobId) {
        try {
            final ReportMailingJobMapper mapper = new ReportMailingJobMapper();
            final String sql = "select " + mapper.ReportMailingJobSchema() + " where rmj.id = ? and rmj.is_deleted = " + sqlResolver.formatBoolValue(false);

            return this.jdbcTemplate.queryForObject(sql, mapper, new Object[] { reportMailingJobId });
        }

        catch (final EmptyResultDataAccessException ex) {
            throw new ReportMailingJobNotFoundException(reportMailingJobId);
        }
    }

    @Override
    public ReportMailingJobData retrieveReportMailingJobEnumOptions() {
        final List<EnumOptionData> emailAttachmentFileFormatOptions = ReportMailingJobEmailAttachmentFileFormat.validOptions();
        final List<EnumOptionData> stretchyReportParamDateOptions = ReportMailingJobStretchyReportParamDateOption.validOptions();

        return ReportMailingJobData.newInstance(emailAttachmentFileFormatOptions, stretchyReportParamDateOptions);
    }

    private static final class ReportMailingJobMapper implements RowMapper<ReportMailingJobData> {

        public static final String FROM = "from m_report_mailing_job rmj "
                + "inner join m_appuser cbu "
                + "on cbu.id = rmj.createdby_id "
                + "left join m_appuser mbu "
                + "on mbu.id = rmj.lastmodifiedby_id "
                + "left join stretchy_report sr "
                + "on rmj.stretchy_report_id = sr.id ";

        public String ReportMailingJobSchema() {
            return "rmj.id, rmj.name, rmj.description, rmj.start_datetime as start_date_time, rmj.recurrence, rmj.created_date as created_on_date, "
                    + "cbu.username as created_by_username, cbu.firstname as created_by_firstname, cbu.lastname as created_by_lastname, "
                    + "rmj.lastmodified_date as updated_on_date, "
                    + "mbu.username as updated_by_username, mbu.firstname as updated_by_firstname, mbu.lastname as updated_by_lastname, "
                    + "rmj.email_recipients as email_recipients, "
                    + "rmj.email_subject as email_subject, rmj.email_message as email_message, "
                    + "rmj.email_attachment_file_format as email_attachment_file_format, "
                    + "rmj.stretchy_report_param_map as stretchy_report_param_map, rmj.previous_run_datetime as previous_run_date_time, "
                    + "rmj.next_run_datetime as next_run_date_time, rmj.previous_run_status as previous_run_status, "
                    + "rmj.previous_run_error_log as previous_run_error_log, rmj.previous_run_error_message as previous_run_error_message, "
                    + "rmj.number_of_runs as number_of_runs, rmj.is_active as is_active, rmj.run_as_userid as run_as_user_id, "
                    + "sr.id as report_id, sr.report_name as report_name, sr.report_type as report_type, sr.report_subtype as report_sub_type, "
                    + "sr.report_category as report_category, sr.report_sql as report_sql, sr.description as report_description, "
                    + "sr.core_report as core_report, sr.use_report as use_report "
                    + FROM;
        }

        public String countSchema() {
            return " count(rmj.*) " + FROM;
        }

        @Override
        public ReportMailingJobData mapRow(ResultSet rs, int rowNum) throws SQLException {
            final Long id = rs.getLong("id");
            final String name = rs.getString("name");
            final String description = rs.getString("description");
            final ZonedDateTime startDateTime = JdbcSupport.getDateTime(rs, "startDateTime");
            final String recurrence = rs.getString("recurrence");
            final LocalDate createdOnDate = JdbcSupport.getLocalDate(rs, "created_on_date");
            final LocalDate updatedOnDate = JdbcSupport.getLocalDate(rs, "updated_on_date");
            final String emailRecipients = rs.getString("email_recipients");
            final String emailSubject = rs.getString("email_subject");
            final String emailMessage = rs.getString("email_message");
            final String emailAttachmentFileFormatString = rs.getString("email_attachment_file_format");
            EnumOptionData emailAttachmentFileFormat = null;

            if (emailAttachmentFileFormatString != null) {
                ReportMailingJobEmailAttachmentFileFormat format = ReportMailingJobEmailAttachmentFileFormat.newInstance(emailAttachmentFileFormatString);

                emailAttachmentFileFormat = format.toEnumOptionData();
            }

            final String stretchyReportParamMap = rs.getString("stretchy_report_param_map");
            final ZonedDateTime previousRunDateTime = JdbcSupport.getDateTime(rs, "previous_run_date_time");
            final ZonedDateTime nextRunDateTime = JdbcSupport.getDateTime(rs, "next_run_date_time");
            final String previousRunStatus = rs.getString("previous_run_status");
            final String previousRunErrorLog = rs.getString("previous_run_error_log");
            final String previousRunErrorMessage = rs.getString("previous_run_error_message");
            final Integer numberOfRuns = JdbcSupport.getInteger(rs, "number_of_runs");
            final boolean isActive = rs.getBoolean("is_active");
            final String createdByUsername = rs.getString("created_by_username");
            final String createdByFirstname = rs.getString("created_by_firstname");
            final String createdByLastname = rs.getString("created_by_lastname");
            final String updatedByUsername = rs.getString("updated_by_username");
            final String updatedByFirstname = rs.getString("updated_by_firstname");
            final String updatedByLastname = rs.getString("updated_by_lastname");
            final ReportMailingJobTimelineData timeline = new ReportMailingJobTimelineData(createdOnDate, createdByUsername,
                    createdByFirstname, createdByLastname, updatedOnDate, updatedByUsername, updatedByFirstname, updatedByLastname);
            final Long runAsUserId = JdbcSupport.getLong(rs, "run_as_user_id");

            final Long reportId = JdbcSupport.getLong(rs, "report_id");
            final String reportName = rs.getString("report_name");
            final String reportType = rs.getString("report_type");
            final String reportSubType = rs.getString("report_sub_type");
            final String reportCategory = rs.getString("report_category");
            final String reportSql = rs.getString("report_sql");
            final String reportDescription = rs.getString("report_description");
            final boolean coreReport = rs.getBoolean("core_report");
            final boolean useReport = rs.getBoolean("use_report");

            final ReportData stretchyReport = new ReportData(reportId, reportName, reportType, reportSubType, reportCategory,
                    reportDescription, reportSql, coreReport, useReport, null);

            return ReportMailingJobData.newInstance(id, name, description, startDateTime, recurrence, timeline, emailRecipients,
                    emailSubject, emailMessage, emailAttachmentFileFormat, stretchyReport, stretchyReportParamMap, previousRunDateTime,
                    nextRunDateTime, previousRunStatus, previousRunErrorLog, previousRunErrorMessage, numberOfRuns, isActive,
                    runAsUserId);
        }
    }
}
