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
package org.apache.fineract.infrastructure.core.service;

import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.lookup.DataSourceLookup;
import org.springframework.jdbc.datasource.lookup.JndiDataSourceLookup;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TenantAwareSpringLiquibase extends SpringLiquibase {

    private static Logger log = LoggerFactory.getLogger(TenantAwareSpringLiquibase.class);

    //    @Nullable
    private Map<Object, Object> targetDataSources;
    //    @Nullable
    private Map<String, DataSource> resolvedDataSources;

    private DataSourceLookup dataSourceLookup = new JndiDataSourceLookup();

    public void setTargetDataSources(/*@Nullable*/ Map<Object, Object> targetDataSources) {
        this.targetDataSources = targetDataSources;
    }

    @Override
    public void afterPropertiesSet() throws LiquibaseException {
        if (getDataSource() != null)
            super.afterPropertiesSet();
        else {
            if (targetDataSources == null) {
                throw new IllegalArgumentException("Property 'targetDataSources' is required");
            }
            log.info("DataSources based multitenancy enabled");
            resolveDataSources();
            runOnAllDataSources();
        }
    }

    protected String resolveSpecifiedLookupKey(Object lookupKey) {
        return String.valueOf(lookupKey);
    }

    private void resolveDataSources() {
        resolvedDataSources = new HashMap<>(targetDataSources.size());
        targetDataSources.forEach((key, value) -> {
            String lookupKey = resolveSpecifiedLookupKey(key);
            DataSource dataSource = resolveSpecifiedDataSource(value);
            resolvedDataSources.put(lookupKey, dataSource);
        });
    }

    protected DataSource resolveSpecifiedDataSource(Object dataSource) throws IllegalArgumentException {
        if (dataSource instanceof DataSource)
            return (DataSource) dataSource;
        else if (dataSource instanceof String)
            return dataSourceLookup.getDataSource((String) dataSource);
        else
            throw new IllegalArgumentException("Illegal data source value - only [javax.sql.DataSource] and String supported: " + dataSource);
    }

    private void runOnAllDataSources() throws LiquibaseException {
        for (Map.Entry<String, DataSource> entry : resolvedDataSources.entrySet()) {
            String key = entry.getKey();
            DataSource value = entry.getValue();
            log.info("Initializing Liquibase for data source " + value);
            SpringLiquibase liquibase = getSpringLiquibase(value);

            String contexts = getContexts();
            contexts = contexts == null || contexts.isEmpty() ? key : (contexts + ", " + key);
            liquibase.setContexts(contexts);

            liquibase.afterPropertiesSet();
            log.info("Liquibase ran for data source " + value);
        }
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        if (resourceLoader == null)
            log.debug(Arrays.toString(Thread.currentThread().getStackTrace()));
        super.setResourceLoader(resourceLoader);
    }

    protected SpringLiquibase getSpringLiquibase(DataSource dataSource) {
        SpringLiquibase liquibase = new SpringLiquibase();
        liquibase.setChangeLog(changeLog);
        liquibase.setChangeLogParameters(parameters);
        liquibase.setContexts(contexts);
        liquibase.setLabels(labels);
        liquibase.setDropFirst(dropFirst);
        liquibase.setShouldRun(shouldRun);
        liquibase.setRollbackFile(rollbackFile);
        liquibase.setResourceLoader(resourceLoader);
        liquibase.setDataSource(dataSource);
        liquibase.setDefaultSchema(defaultSchema);
        liquibase.setLiquibaseSchema(liquibaseSchema);
        liquibase.setLiquibaseTablespace(liquibaseTablespace);
        liquibase.setDatabaseChangeLogTable(databaseChangeLogTable);
        liquibase.setDatabaseChangeLogLockTable(databaseChangeLogLockTable);
        return liquibase;
    }
}

