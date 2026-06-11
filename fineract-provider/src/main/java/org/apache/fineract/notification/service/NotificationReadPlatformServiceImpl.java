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

import java.util.HashMap;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.core.service.Page;
import org.apache.fineract.infrastructure.core.service.SearchParameters;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.apache.fineract.notification.cache.CacheNotificationResponseHeader;
import org.apache.fineract.notification.data.NotificationData;
import org.apache.fineract.notification.domain.NotificationMapperRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Transactional;

@RequiredArgsConstructor
public class NotificationReadPlatformServiceImpl implements NotificationReadPlatformService {

    private HashMap<Long, HashMap<Long, CacheNotificationResponseHeader>> tenantNotificationResponseHeaderCache = new HashMap<>();

    private final PlatformSecurityContext context;
    private final NotificationMapperRepository notificationMapperRepository;

    @Override
    @Transactional(readOnly = true)
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
        return this.notificationMapperRepository.hasUnreadNotifications(appUserId);
    }

    @Override
    public void updateNotificationReadStatus() {
        final Long appUserId = context.authenticatedUser().getId();
        this.notificationMapperRepository.markUnreadNotificationsAsRead(appUserId);
    }

    @Override
    public Page<NotificationData> getAllUnreadNotifications(final SearchParameters searchParameters) {
        final Long appUserId = context.authenticatedUser().getId();
        final Pageable pageable = toPageable(searchParameters);
        final org.springframework.data.domain.Page<NotificationData> springPage = this.notificationMapperRepository
                .findNotificationDataByUserIdAndReadStatus(appUserId, false, pageable);
        return toFineractPage(springPage);
    }

    @Override
    public Page<NotificationData> getAllNotifications(final SearchParameters searchParameters) {
        final Long appUserId = context.authenticatedUser().getId();
        final Pageable pageable = toPageable(searchParameters);
        final org.springframework.data.domain.Page<NotificationData> springPage = this.notificationMapperRepository
                .findNotificationDataByUserIdAndReadStatus(appUserId, null, pageable);
        return toFineractPage(springPage);
    }

    private Pageable toPageable(final SearchParameters searchParameters) {
        final int limit = searchParameters.hasLimit() ? searchParameters.getLimit() : SearchParameters.DEFAULT_MAX_LIMIT;
        final int offset = searchParameters.hasOffset() ? searchParameters.getOffset() : 0;

        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be greater than zero");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }

        final int page = offset / limit;

        Sort sort = Sort.by(Sort.Direction.DESC, "createdAt");

        if (searchParameters.hasOrderBy()) {
            final String orderBy = searchParameters.getOrderBy();
            final Sort.Direction direction = "ASC".equalsIgnoreCase(searchParameters.getSortOrder()) ? Sort.Direction.ASC
                    : Sort.Direction.DESC;
            sort = Sort.by(direction, orderBy);
        }

        return PageRequest.of(page, limit, sort);
    }

    private Page<NotificationData> toFineractPage(final org.springframework.data.domain.Page<NotificationData> springPage) {
        return new Page<>(springPage.getContent(), Math.toIntExact(springPage.getTotalElements()));
    }
}
