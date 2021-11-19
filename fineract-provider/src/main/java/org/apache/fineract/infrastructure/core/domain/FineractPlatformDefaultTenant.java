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
package org.apache.fineract.infrastructure.core.domain;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class FineractPlatformDefaultTenant {
    
    @Value("${FINERACT_DEFAULT_TENANT_ID:1}")
    private Long id;

    @Value("${FINERACT_DEFAULT_TENANT_IDENTIFIER:default}")
    private String tenantIdentifier;

    @Value("${FINERACT_DEFAULT_TENANT_NAME:Default Demo Tenant}")
    private String name;

    @Value("${FINERACT_DEFAULT_TENANT_TIMEZONE:Asia/Kolkata}")
    private String timezoneId;

    @Value("${FINERACT_DEFAULT_CONNECTION_ID:1}")
    private Long connectionId;

    @Value("${FINERACT_DEFAULT_CONNECTION_SCHEMA_SERVER:postgresql}")
    private String schemaServer;

    @Value("${FINERACT_DEFAULT_CONNECTION_SCHEMA_SERVER_PORT:5432}")
    private String schemaServerPort;

    @Value("${FINERACT_DEFAULT_CONNECTION_SCHEMA_CONNECTION_PARAMETERS:}")
    private String schemaConnectionParameters;
    
    @Value("${FINERACT_DEFAULT_CONNECTION_SCHEMA_USERNAME:postgres}")
    private String schemaUsername;

    @Value("${FINERACT_DEFAULT_CONNECTION_SCHEMA_PASSWORD:password}")
    private String schemaPassword;

    @Value("${FINERACT_DEFAULT_CONNECTION_SCHEMA_NAME:fineract_default}")
    private String schemaName;

    @Value("${FINERACT_DEFAULT_CONNECTION_AUTOUPDATE_ENABLED:true}")
    private boolean autoUpdateEnabled;

    @Value("${FINERACT_DEFAULT_CONNECTION_INITIAL_SIZE:5}")
    private int initialSize;

    @Value("${FINERACT_DEFAULT_CONNECTION_VALIDATION_INTERVAL:30000}")
    private long validationInterval;

    @Value("${FINERACT_DEFAULT_CONNECTION_REMOVE_ABANDONED:true}")
    private boolean removeAbandoned;

    @Value("${FINERACT_DEFAULT_CONNECTION_REMOVE_ABANDONED_TIMEOUT:60}")
    private int removeAbandonedTimeout;

    @Value("${FINERACT_DEFAULT_CONNECTION_LOG_ABANDONED:true}")
    private boolean logAbandoned;

    @Value("${FINERACT_DEFAULT_CONNECTION_ABANDON_PERCENTAGE_FULL:50}")
    private int abandonWhenPercentageFull;

    @Value("${FINERACT_DEFAULT_CONNECTION_MAXACTIVE:40}")
    private int maxActive;

    @Value("${FINERACT_DEFAULT_CONNECTION_MINIDLE:50}")
    private int minIdle;

    @Value("${FINERACT_DEFAULT_CONNECTION_MAXIDLE:10}")
    private int maxIdle;

    @Value("${FINERACT_DEFAULT_CONNECTION_SUSPECT_TIMEOUT:60}")
    private int suspectTimeout;

    @Value("${FINERACT_DEFAULT_CONNECTION_TIME_BETWEEN_EVICTION_RUNS_MILLIS:34000}")
    private int timeBetweenEvictionRunsMillis;

    @Value("${FINERACT_DEFAULT_CONNECTION_MIN_EVICTABLE_IDLE_TIME_MILLIS:60000}")
    private int minEvictableIdleTimeMillis;

    @Value("${FINERACT_DEFAULT_CONNECTION_MAX_RETRIES_ON_DEAD_LOCK:0}")
    private int maxRetriesOnDeadlock;

    @Value("${FINERACT_DEFAULT_CONNECTION_MAX_INTERVAL_BETWEEN_RETRIES:1}")
    private int maxIntervalBetweenRetries;

    @Value("${FINERACT_DEFAULT_CONNECTION_TEST_ON_BORROW:true}")
    private boolean testOnBorrow;

    @Value("${FINERACT_DEFAULT_CONNECTION_READ_ONLY_SCHEMA_SERVER:}")
    private String readOnlySchemaServer;

    @Value("${FINERACT_DEFAULT_CONNECTION_READ_ONLY_SCHEMA_SERVER_PORT:}")
    private String readOnlySchemaServerPort;

    @Value("${FINERACT_DEFAULT_CONNECTION_READ_ONLY_SCHEMA_NAME:}")
    private String readOnlySchemaName;

    @Value("${FINERACT_DEFAULT_CONNECTION_READ_ONLY_SCHEMA_USERNAME:}")
    private String readOnlySchemaUsername;

    @Value("${FINERACT_DEFAULT_CONNECTION_READ_ONLY_SCHEMA_PASSWORD:}")
    private String readOnlySchemaPassword;

    public Long getId() {
        return this.id;
    }

    public String getTenantIdentifier() {
        return this.tenantIdentifier;
    }

    public String getName() {
        return this.name;
    }

    public String getTimezoneId() {
        return this.timezoneId;
    }

    public final FineractPlatformTenant getFineractTenant(){
        return new FineractPlatformTenant(id, tenantIdentifier, name, timezoneId,getConnection()); 
    }

    public final FineractPlatformTenantConnection getConnection() {
        return new FineractPlatformTenantConnection(connectionId, schemaName, schemaServer,
        schemaServerPort, schemaConnectionParameters, schemaUsername,
        schemaPassword, autoUpdateEnabled, initialSize, validationInterval,
        removeAbandoned, removeAbandonedTimeout, logAbandoned,
        abandonWhenPercentageFull, maxActive, minIdle, maxIdle, suspectTimeout,
        timeBetweenEvictionRunsMillis, minEvictableIdleTimeMillis, maxRetriesOnDeadlock,
        maxIntervalBetweenRetries, testOnBorrow, readOnlySchemaServer,
        readOnlySchemaServerPort, readOnlySchemaName, readOnlySchemaUsername,
        readOnlySchemaPassword);
    }
}
