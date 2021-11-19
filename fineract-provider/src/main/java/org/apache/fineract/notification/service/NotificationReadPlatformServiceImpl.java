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
package org.apache.fineract.notification.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.service.Page;
import org.apache.fineract.infrastructure.core.service.PaginationHelper;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.infrastructure.core.service.SearchParameters;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.apache.fineract.infrastructure.security.utils.ColumnValidator;
import org.apache.fineract.notification.cache.CacheNotificationResponseHeader;
import org.apache.fineract.notification.data.NotificationData;
import org.apache.fineract.notification.data.NotificationMapperData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class NotificationReadPlatformServiceImpl implements NotificationReadPlatformService {

    private final JdbcTemplate jdbcTemplate;
    private final DataSourceSqlResolver sqlResolver;
    private final PlatformSecurityContext context;
    private final ColumnValidator columnValidator;
    private final PaginationHelper<NotificationData> paginationHelper = new PaginationHelper<>();
    private final NotificationDataRow notificationDataRow = new NotificationDataRow();
    private final NotificationMapperRow notificationMapperRow = new NotificationMapperRow();
    private HashMap<Long, HashMap<Long, CacheNotificationResponseHeader>> tenantNotificationResponseHeaderCache = new HashMap<>();

    @Autowired
    public NotificationReadPlatformServiceImpl(final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver,
                                               final PlatformSecurityContext context, final ColumnValidator columnValidator) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.sqlResolver = sqlResolver;
        this.context = context;
        this.columnValidator = columnValidator;
    }

    @Override
    public boolean hasUnreadNotifications(Long appUserId) {
        Long tenantId = ThreadLocalContextUtil.getTenant().getId();
        Long now = System.currentTimeMillis() / 1000L;
        if (this.tenantNotificationResponseHeaderCache.containsKey(tenantId)) {
            HashMap<Long, CacheNotificationResponseHeader> notificationResponseHeaderCache = this.tenantNotificationResponseHeaderCache
                    .get(tenantId);
            if (notificationResponseHeaderCache.containsKey(appUserId)) {
                Long lastFetch = notificationResponseHeaderCache.get(appUserId).getLastFetch();
                if ((now - lastFetch) > 1) {
                    return this.createUpdateCacheValue(appUserId, now, notificationResponseHeaderCache);
                } else {
                    return notificationResponseHeaderCache.get(appUserId).hasNotifications();
                }
            } else {
                return this.createUpdateCacheValue(appUserId, now, notificationResponseHeaderCache);
            }
        } else {
            return this.initializeTenantNotificationResponseHeaderCache(tenantId, now, appUserId);

        }
    }

    private boolean initializeTenantNotificationResponseHeaderCache(Long tenantId, Long now, Long appUserId) {
        HashMap<Long, CacheNotificationResponseHeader> notificationResponseHeaderCache = new HashMap<>();
        this.tenantNotificationResponseHeaderCache.put(tenantId, notificationResponseHeaderCache);
        return this.createUpdateCacheValue(appUserId, now, notificationResponseHeaderCache);
    }

    private boolean createUpdateCacheValue(Long appUserId, Long now,
            HashMap<Long, CacheNotificationResponseHeader> notificationResponseHeaderCache) {
        boolean hasNotifications;
        Long tenantId = ThreadLocalContextUtil.getTenant().getId();
        CacheNotificationResponseHeader cacheNotificationResponseHeader;
        hasNotifications = checkForUnreadNotifications(appUserId);
        cacheNotificationResponseHeader = new CacheNotificationResponseHeader(hasNotifications, now);
        notificationResponseHeaderCache.put(appUserId, cacheNotificationResponseHeader);
        this.tenantNotificationResponseHeaderCache.put(tenantId, notificationResponseHeaderCache);
        return hasNotifications;
    }

    private boolean checkForUnreadNotifications(Long appUserId) {
        String sql = "SELECT EXISTS(SELECT 1 FROM notification_mapper WHERE user_id = ? AND is_read = " + sqlResolver.formatBoolValue(
                false) + ")";
        return Boolean.TRUE.equals(this.jdbcTemplate.queryForObject(sql, Boolean.class, appUserId));
    }

    @Override
    public void updateNotificationReadStatus() {
        final Long appUserId = context.authenticatedUser().getId();
        String sql = "UPDATE notification_mapper SET is_read = " + sqlResolver.formatBoolValue(true) + " WHERE is_read = " + sqlResolver.formatBoolValue(false)
                + " and user_id = ?";
        this.jdbcTemplate.update(sql, appUserId);
    }

    @Override
    public Page<NotificationData> getAllUnreadNotifications(final SearchParameters searchParameters) {
        final StringBuilder sqlBuilder = new StringBuilder(500);
        sqlBuilder.append("SELECT ");
        boolean mySql = sqlResolver.getDialect().isMySql();
        if (mySql)
            sqlBuilder.append("SQL_CALC_FOUND_ROWS ");

        final String where = "WHERE nm.user_id = ? AND nm.is_read = " + sqlResolver.formatBoolValue(false);

        final Long appUserId = context.authenticatedUser().getId();
        sqlBuilder.append(notificationMapperRow.schema())
                .append(where);

        final String sqlCountRows = mySql ? "SELECT FOUND_ROWS()" : (notificationMapperRow.countSchema() + where);
        return getNotificationDataPage(searchParameters, appUserId, sqlBuilder.toString(), sqlCountRows);
    }


    @Override
    public Page<NotificationData> getAllNotifications(SearchParameters searchParameters) {
        final StringBuilder sqlBuilder = new StringBuilder(500)
                .append("SELECT ");
        boolean mySql = sqlResolver.getDialect().isMySql();
        if (mySql)
            sqlBuilder.append("SQL_CALC_FOUND_ROWS ");

        final String where = "WHERE nm.user_id = ? ";

        final Long appUserId = context.authenticatedUser().getId();
        sqlBuilder.append(notificationMapperRow.schema())
                .append(where);

        final String sqlCountRows = mySql ? "SELECT FOUND_ROWS()" : (notificationMapperRow.countSchema() + where);
        return getNotificationDataPage(searchParameters, appUserId, sqlBuilder.toString(), sqlCountRows);
    }

    private Page<NotificationData> getNotificationDataPage(SearchParameters searchParameters, Long appUserId, String sql, String sqlCountRows) {
        final StringBuilder sqlBuilder = new StringBuilder(200);
        sqlBuilder.append(sql);

        if (searchParameters.isOrderByRequested()) {
            sqlBuilder.append(" order by ").append(searchParameters.getOrderBy());
            this.columnValidator.validateSqlInjection(sqlBuilder.toString(), searchParameters.getOrderBy());
            if (searchParameters.isSortOrderProvided()) {
                sqlBuilder.append(' ').append(searchParameters.getSortOrder());
                this.columnValidator.validateSqlInjection(sqlBuilder.toString(), searchParameters.getSortOrder());
            }
        } else
            sqlBuilder.append(" order by nm.created_at desc");

        if (searchParameters.isLimited()) {
            sqlBuilder.append(" limit ").append(searchParameters.getLimit());
            if (searchParameters.isOffset()) {
                sqlBuilder.append(" offset ").append(searchParameters.getOffset());
            }
        }

        final Object[] sqlParams = new Object[]{appUserId};
        Object[] countParams = null;
        if (!sqlResolver.getDialect().isMySql()) {
            countParams = sqlParams;
        }
        return this.paginationHelper.fetchPage(this.jdbcTemplate, sqlCountRows, countParams, sqlBuilder.toString(), sqlParams, this.notificationDataRow);
    }

    private static final class NotificationMapperRow implements RowMapper<NotificationMapperData> {
        public static final String FROM = " FROM notification_mapper nm INNER JOIN notification_generator ng ON nm.notification_id = ng.id ";

        public static final String SCHEMA =  " ng.id as id, nm.user_id as user_id, ng.object_type as object_type, ng.object_identifier as object_identifier, ng.actor as actor, " +
                "ng.action as action, ng.notification_content as content, ng.is_system_generated as is_system_generated, nm.created_at as created_at ";

        public String schema() {
            return SCHEMA + FROM;
        }

        public String countSchema() {
            return "SELECT count(nm.*) " + FROM;
        }

        @Override
        public NotificationMapperData mapRow(ResultSet rs, int rowNum) throws SQLException {
            NotificationMapperData notificationMapperData = new NotificationMapperData();

            final Long id = rs.getLong("id");
            notificationMapperData.setId(id);

            final Long notificationId = rs.getLong("notification_id");
            notificationMapperData.setNotificationId(notificationId);

            final Long userId = rs.getLong("user_id");
            notificationMapperData.setUserId(userId);

            final boolean isRead = rs.getBoolean("is_read");
            notificationMapperData.setRead(isRead);

            final String createdAt = rs.getString("created_at");
            notificationMapperData.setCreatedAt(createdAt);

            return notificationMapperData;
        }
    }

    private static final class NotificationDataRow implements RowMapper<NotificationData> {

        @Override
        public NotificationData mapRow(ResultSet rs, int rowNum) throws SQLException {
            NotificationData notificationData = new NotificationData();

            final Long id = rs.getLong("id");
            notificationData.setId(id);

            final String objectType = rs.getString("object_type");
            notificationData.setObjectType(objectType);

            final Long objectIdentifier = rs.getLong("object_identifier");
            notificationData.setObjectIdentifier(objectIdentifier);

            final Long actorId = rs.getLong("actor");
            notificationData.setActor(actorId);

            final String action = rs.getString("action");
            notificationData.setAction(action);

            final String content = rs.getString("content");
            notificationData.setContent(content);

            final boolean isSystemGenerated = rs.getBoolean("is_system_generated");
            notificationData.setSystemGenerated(isSystemGenerated);

            final String createdAt = rs.getString("created_at");
            notificationData.setCreatedAt(createdAt);

            return notificationData;
        }
    }
}
