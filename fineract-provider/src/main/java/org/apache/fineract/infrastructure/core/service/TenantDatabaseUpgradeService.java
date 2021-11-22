/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.fineract.infrastructure.core.service;

import com.zaxxer.hikari.HikariDataSource;
import liquibase.exception.LiquibaseException;
import org.apache.fineract.infrastructure.core.boot.JDBCDriverConfig;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenantConnection;
import org.apache.fineract.infrastructure.core.utils.ProfileUtils;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.apache.fineract.infrastructure.security.service.TenantDetailsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;

/**
 * A service that picks up on tenants that are configured to auto-update their specific schema on application startup.
 */
@Service
public class TenantDatabaseUpgradeService {

    private final static Logger LOG = LoggerFactory.getLogger(TenantDatabaseUpgradeService.class);

    private final TenantDetailsService tenantDetailsService;
    private JDBCDriverConfig driverConfig;
    private ProfileUtils profileUtils;
    private final LiquibaseProperties liquibaseProperties;
    private final ResourceLoader resourceLoader;
    protected final DataSource dataSource;

    @Autowired
    public TenantDatabaseUpgradeService(final TenantDetailsService detailsService,
                                        JDBCDriverConfig jdbcDriverConfig,
                                        LiquibaseProperties liquibaseProperties,
                                        ResourceLoader resourceLoader,
                                        @Qualifier("hikariTenantDataSource") final DataSource dataSource,
                                        final ApplicationContext context) {
        this.tenantDetailsService = detailsService;
        this.driverConfig = jdbcDriverConfig;
        this.liquibaseProperties = liquibaseProperties;
        this.resourceLoader = resourceLoader;
        this.dataSource = dataSource;
        this.profileUtils = new ProfileUtils(context.getEnvironment());
    }

    @PostConstruct
    public void upgradeAllTenants() throws LiquibaseException {
        if (!liquibaseProperties.isEnabled()) {
            return;
        }
        if (profileUtils.isActiveProfile(JobConstants.SPRING_UPGRADEDB_PROFILE_NAME)) {
            upgradeTenantsDb();

            final List<FineractPlatformTenant> tenants = this.tenantDetailsService.findAllTenants();
            if (tenants == null) {
                return;
            }
            TenantAwareSpringLiquibase liquibase = createLiquibase(profileUtils.calcContext(this.driverConfig,
                 new StringBuilder("core_db")).toString());

            HashMap<Object, Object> dataSources = new HashMap<>();
            for (FineractPlatformTenant tenant : tenants) {
                FineractPlatformTenantConnection tenantConnection = tenant.getConnection();
                LOG.debug("Initialize liquibase for tenant " + tenant.getTenantIdentifier());
                dataSources.put(tenant.getTenantIdentifier(), createTenantDataSource((HikariDataSource) dataSource, tenantConnection));
            }
            liquibase.setTargetDataSources(dataSources);
            LOG.debug("Start liquibase on core database");
            liquibase.afterPropertiesSet();
        }
    }

    /**
     * Initializes, and if required upgrades (using Liquibase) the Tenants DB itself.
     */
    private void upgradeTenantsDb() throws LiquibaseException {
        TenantAwareSpringLiquibase liquibase = createLiquibase(profileUtils.calcContext(this.driverConfig,
            new StringBuilder("tenants_db")).toString());

        liquibase.setDataSource(dataSource);
        LOG.debug("Start liquibase on list database " + dataSource);

        liquibase.afterPropertiesSet();
    }

    private TenantAwareSpringLiquibase createLiquibase(String context) {
        TenantAwareSpringLiquibase liquibase = new TenantAwareSpringLiquibase();
        String changeLog = liquibaseProperties.getChangeLog();

        liquibase.setShouldRun(true);
        liquibase.setChangeLog(changeLog);
        String contexts = liquibaseProperties.getContexts();
        contexts = contexts == null || contexts.isEmpty() ? context : (contexts + ", " + context);
        liquibase.setContexts(contexts);
        liquibase.setDefaultSchema(liquibaseProperties.getDefaultSchema());
        liquibase.setDropFirst(liquibaseProperties.isDropFirst());
        liquibase.setResourceLoader(resourceLoader);
        liquibase.setChangeLogParameters(liquibaseProperties.getParameters());

        LOG.debug("Initiated liquibase " + liquibase.getContexts());
        return liquibase;
    }

    private DataSource createTenantDataSource(HikariDataSource tenantsDataSource, FineractPlatformTenantConnection tenantConnection) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(tenantsDataSource.getDriverClassName());
        dataSource.setDataSourceProperties(tenantsDataSource.getDataSourceProperties());
        dataSource.setMinimumIdle(tenantsDataSource.getMinimumIdle());
        dataSource.setMaximumPoolSize(tenantsDataSource.getMaximumPoolSize());
        dataSource.setIdleTimeout(tenantsDataSource.getIdleTimeout());
        dataSource.setConnectionTestQuery(tenantsDataSource.getConnectionTestQuery());

        dataSource.setUsername(tenantConnection.getSchemaUsername());
        dataSource.setPassword(tenantConnection.getSchemaPassword());
        dataSource.setJdbcUrl(driverConfig.constructUrl(tenantConnection.getSchemaServer(), tenantConnection.getSchemaServerPort(),
                tenantConnection.getSchemaName(), tenantConnection.getSchemaConnectionParameters()));

        return dataSource;
    }
}