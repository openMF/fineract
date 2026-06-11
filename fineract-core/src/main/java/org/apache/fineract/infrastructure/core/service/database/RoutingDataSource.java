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
package org.apache.fineract.infrastructure.core.service.database;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Based on springs {@link AbstractRoutingDataSource} idea, this is a {@link DataSource} that routes or delegates to
 * another data source depending on the tenant details passed in the request.
 *
 * The tenant details are process earlier and stored in a {@link ThreadLocal}.
 *
 * The {@link RoutingDataSourceService} is responsible for returning the appropriate {@link DataSource} for the tenant
 * of this request.
 */
@Service(value = "dataSource")
@Primary
@Slf4j
public class RoutingDataSource extends AbstractDataSource {

    @Autowired
    private RoutingDataSourceServiceFactory dataSourceServiceFactory;

    @Value("${fineract.datasource.connection-checkout-diagnostics.enabled:false}")
    private boolean connectionCheckoutDiagnosticsEnabled;

    @Value("${fineract.datasource.connection-checkout-diagnostics.stack-depth:12}")
    private int connectionCheckoutDiagnosticsStackDepth;

    @Override
    public Connection getConnection() throws SQLException {
        DataSource targetDataSource = determineTargetDataSource();
        try {
            Connection connection = targetDataSource.getConnection();
            logConnectionCheckout(targetDataSource, null);
            return connection;
        } catch (SQLException e) {
            logConnectionCheckoutFailure(targetDataSource, null, e);
            throw e;
        }
    }

    public DataSource determineTargetDataSource() {
        return this.dataSourceServiceFactory.determineDataSourceService().retrieveDataSource();
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        DataSource targetDataSource = determineTargetDataSource();
        try {
            Connection connection = targetDataSource.getConnection(username, password);
            logConnectionCheckout(targetDataSource, username);
            return connection;
        } catch (SQLException e) {
            logConnectionCheckoutFailure(targetDataSource, username, e);
            throw e;
        }
    }

    private void logConnectionCheckout(DataSource targetDataSource, String username) {
        if (!connectionCheckoutDiagnosticsEnabled) {
            return;
        }
        log.debug("Tenant datasource connection checkout: tenant={}, username={}, transaction={}, hikari={}, stack={}", tenant(),
                username == null ? "<default>" : username, transactionState(), hikariState(targetDataSource), compactStack());
    }

    private void logConnectionCheckoutFailure(DataSource targetDataSource, String username, SQLException exception) {
        if (!connectionCheckoutDiagnosticsEnabled) {
            return;
        }
        log.warn("Tenant datasource connection checkout failed: tenant={}, username={}, transaction={}, hikari={}, error={}, stack={}",
                tenant(), username == null ? "<default>" : username, transactionState(), hikariState(targetDataSource),
                exception.getMessage(), compactStack());
    }

    private String tenant() {
        FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        if (tenant == null) {
            return "<none>";
        }
        if (tenant.getConnection() != null && tenant.getConnection().getConnectionId() != null) {
            return tenant.getTenantIdentifier() + "/connection:" + tenant.getConnection().getConnectionId();
        }
        return tenant.getTenantIdentifier();
    }

    private String transactionState() {
        return "actualActive=" + TransactionSynchronizationManager.isActualTransactionActive() + ", synchronizationActive="
                + TransactionSynchronizationManager.isSynchronizationActive() + ", readOnly="
                + TransactionSynchronizationManager.isCurrentTransactionReadOnly() + ", name="
                + TransactionSynchronizationManager.getCurrentTransactionName();
    }

    private String hikariState(DataSource targetDataSource) {
        if (!(targetDataSource instanceof HikariDataSource hikariDataSource)) {
            return targetDataSource.getClass().getName();
        }
        HikariPoolMXBean poolMXBean = hikariDataSource.getHikariPoolMXBean();
        if (poolMXBean == null) {
            return "poolName=" + hikariDataSource.getPoolName() + ", mxBean=<unavailable>";
        }
        return "poolName=" + hikariDataSource.getPoolName() + ", total=" + poolMXBean.getTotalConnections() + ", active="
                + poolMXBean.getActiveConnections() + ", idle=" + poolMXBean.getIdleConnections() + ", waiting="
                + poolMXBean.getThreadsAwaitingConnection();
    }

    private String compactStack() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        StringBuilder builder = new StringBuilder();
        int added = 0;
        for (StackTraceElement element : stackTrace) {
            String className = element.getClassName();
            if (className.equals(Thread.class.getName()) || className.equals(RoutingDataSource.class.getName())) {
                continue;
            }
            if (added > 0) {
                builder.append(" <- ");
            }
            builder.append(className).append('.').append(element.getMethodName()).append(':').append(element.getLineNumber());
            added++;
            if (added >= connectionCheckoutDiagnosticsStackDepth) {
                break;
            }
        }
        return builder.toString();
    }
}
