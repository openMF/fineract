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
package org.apache.fineract.notification.domain;

import org.apache.fineract.notification.data.NotificationData;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface NotificationMapperRepository extends JpaRepository<NotificationMapper, Long> {

    @Query("""
            select case when count(nm) > 0 then true else false end
            from NotificationMapper nm
            where nm.userId.id = :appUserId
            and nm.isRead = false
            """)
    boolean hasUnreadNotifications(@Param("appUserId") Long appUserId);

    @Modifying
    @Transactional
    @Query("""
            update NotificationMapper nm
            set nm.isRead = true
            where nm.userId.id = :appUserId
            and nm.isRead = false
            """)
    void markUnreadNotificationsAsRead(@Param("appUserId") Long appUserId);

    @Query(value = """
            select new org.apache.fineract.notification.data.NotificationData(
                nm.notification.id,
                nm.notification.objectType,
                nm.notification.objectIdentifier,
                nm.notification.actorId,
                nm.notification.action,
                nm.notification.notificationContent,
                nm.notification.isSystemGenerated,
                nm.isRead,
                nm.createdAt
            )
            from NotificationMapper nm
            where nm.userId.id = :appUserId
            """, countQuery = """
            select count(nm)
            from NotificationMapper nm
            where nm.userId.id = :appUserId
            """)
    Page<NotificationData> findNotificationDataByUserId(@Param("appUserId") Long appUserId, Pageable pageable);

    @Query(value = """
            select new org.apache.fineract.notification.data.NotificationData(
                nm.notification.id,
                nm.notification.objectType,
                nm.notification.objectIdentifier,
                nm.notification.actorId,
                nm.notification.action,
                nm.notification.notificationContent,
                nm.notification.isSystemGenerated,
                nm.isRead,
                nm.createdAt
            )
            from NotificationMapper nm
            where nm.userId.id = :appUserId
            and nm.isRead = false
            """, countQuery = """
            select count(nm)
            from NotificationMapper nm
            where nm.userId.id = :appUserId
            and nm.isRead = false
            """)
    Page<NotificationData> findUnreadNotificationDataByUserId(@Param("appUserId") Long appUserId, Pageable pageable);
}
