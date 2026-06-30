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
package org.apache.fineract.infrastructure.core.persistence.visibility;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.apache.fineract.infrastructure.core.persistence.ExtendedJpaTransactionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.EntityManagerFactoryUtils;
import org.springframework.orm.jpa.EntityManagerHolder;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.EclipseLinkJpaVendorAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real (non-mocked) test of whether JPA (EclipseLink {@link EntityManager}) and direct JDBC ({@link JdbcTemplate}) run
 * on the same physical connection inside one transaction - and therefore whether they can read each other's
 * flushed-but-not-yet-committed writes. It runs against a real PostgreSQL (via Testcontainers) with a real EclipseLink
 * EntityManagerFactory, a real {@link ExtendedJpaTransactionManager}, and a real {@link JdbcTemplate} on the same
 * {@link DataSource}.
 *
 * <p>
 * Confirmed finding: within one transaction, JPA and JDBC share a single physical connection, so JdbcTemplate reads see
 * the transaction's flushed-but-uncommitted JPA writes and both commit/roll back atomically together. This holds both
 * with and without an explicit {@code setDataSource(...)} on the transaction manager - i.e. binding the DataSource is
 * NOT what causes the sharing; {@link org.springframework.orm.jpa.JpaTransactionManager} makes EclipseLink's connection
 * available to plain JDBC regardless (it derives the DataSource from the EntityManagerFactory). Because the connection
 * is shared once materialised, running EclipseLink with {@code lazyDatabaseTransaction=true} is safe: lazy mode only
 * defers <em>when</em> that single connection is acquired, not whether JPA and JDBC share it.
 *
 * <p>
 * The sharing is established two ways for certainty:
 * <ul>
 * <li>comparing {@code pg_backend_pid()} (unique per PostgreSQL backend/connection) obtained via the EntityManager vs
 * via the JdbcTemplate - equal PIDs prove the same physical connection, and</li>
 * <li>a control query on a genuinely separate raw connection taken directly from the pool (bypassing Spring): under
 * READ_COMMITTED it must NOT see the uncommitted row, proving the JPA write is truly uncommitted and ruling out a
 * premature/auto-commit explanation for the visibility observed via JdbcTemplate.</li>
 * </ul>
 *
 * <p>
 * Requires a Docker daemon; the whole class is skipped automatically when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
public class JpaJdbcSameTransactionVisibilityTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private static final AtomicInteger DB_COUNTER = new AtomicInteger();

    private final List<LocalContainerEntityManagerFactoryBean> createdFactories = new ArrayList<>();
    private final List<HikariDataSource> createdDataSources = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        createdFactories.forEach(LocalContainerEntityManagerFactoryBean::destroy);
        createdFactories.clear();
        createdDataSources.forEach(HikariDataSource::close);
        createdDataSources.clear();
    }

    @Test
    public void withoutDataSourceBoundOnManager() {
        ScenarioResult result = runScenario(createFixture(false));

        assertResultsAreConsistent(result);
        // Mirrors Fineract's primary transaction manager (no explicit setDataSource). JPA and JDBC still run on the
        // same
        // physical PostgreSQL backend: JpaTransactionManager exposes EclipseLink's connection to plain JDBC even
        // without
        // an explicit DataSource binding (it derives the DataSource from the EntityManagerFactory).
        assertThat(result.jpaBackendPid()).as("""
                JdbcTemplate must run on the same physical PostgreSQL backend as EclipseLink,
                so it can read the JPA transaction's uncommitted writes""").isEqualTo(result.jdbcBackendPid());
        assertThat(result.jdbcCountInsideTransaction())
                .as("sharing the connection, JdbcTemplate sees the flushed-but-uncommitted JPA row in the same transaction").isEqualTo(1);
    }

    @Test
    public void withDataSourceBoundOnManager() {
        ScenarioResult result = runScenario(createFixture(true));

        assertResultsAreConsistent(result);
        assertThat(result.jpaBackendPid()).as("with the DataSource bound, JdbcTemplate shares EclipseLink's physical connection")
                .isEqualTo(result.jdbcBackendPid());
        assertThat(result.jdbcCountInsideTransaction())
                .as("sharing the connection, JdbcTemplate sees the flushed-but-uncommitted JPA row in the same transaction").isEqualTo(1);
    }

    /**
     * Invariants that hold regardless of connection sharing: the JPA write is real but genuinely uncommitted (a
     * separate raw connection cannot see it), and it becomes visible to everyone only after commit. The
     * separate-connection check is the decisive control that rules out a premature/auto-commit explanation for any
     * visibility observed elsewhere.
     */
    private void assertResultsAreConsistent(ScenarioResult result) {
        assertThat(result.jpaCountInsideTransaction()).as("JPA must see its own flushed row").isEqualTo(1L);
        assertThat(result.separateRawConnectionCountInsideTransaction())
                .as("a genuinely separate connection (taken straight from the pool, outside Spring) must NOT see the row - "
                        + "this proves the JPA write is still uncommitted and was NOT committed early")
                .isZero();
        assertThat(result.jdbcCountAfterCommit()).as("after commit the row is visible to everyone").isEqualTo(1);
    }

    private ScenarioResult runScenario(Fixture fixture) {
        long[] jpaCount = new long[1];
        int[] jdbcCountInside = new int[1];
        int[] separateRawCount = new int[1];
        long[] jpaPid = new long[1];
        long[] jdbcPid = new long[1];

        // Guard: the transaction is actually driven by the real ExtendedJpaTransactionManager, not some other manager.
        assertThat(fixture.transactionTemplate().getTransactionManager())
                .as("the test must drive the transaction with Fineract's ExtendedJpaTransactionManager")
                .isInstanceOf(ExtendedJpaTransactionManager.class);

        fixture.transactionTemplate().executeWithoutResult(status -> {
            // Guard: a JPA transaction manager binds an EntityManagerHolder to the EMF (a JDBC-only
            // DataSourceTransactionManager
            // would not), proving this transaction is genuinely JPA-managed.
            assertThat(TransactionSynchronizationManager.getResource(fixture.entityManagerFactoryBean().getObject()))
                    .as("a JPA transaction manager must bind an EntityManagerHolder to the EntityManagerFactory")
                    .isInstanceOf(EntityManagerHolder.class);

            EntityManager em = EntityManagerFactoryUtils.getTransactionalEntityManager(fixture.entityManagerFactoryBean().getObject());
            em.persist(new VisibilityProbe(1L, "probe"));
            em.flush();

            jpaCount[0] = em.createQuery("select count(p) from VisibilityProbe p", Long.class).getSingleResult();
            jpaPid[0] = ((Number) em.createNativeQuery("select pg_backend_pid()").getSingleResult()).longValue();

            jdbcCountInside[0] = countViaJdbc(fixture.jdbcTemplate());
            Long pid = fixture.jdbcTemplate().queryForObject("select pg_backend_pid()", Long.class);
            jdbcPid[0] = pid == null ? -1L : pid;

            separateRawCount[0] = countViaSeparateRawConnection(fixture.dataSource());
        });

        int jdbcCountAfterCommit = countViaJdbc(fixture.jdbcTemplate());
        return new ScenarioResult(jpaCount[0], jdbcCountInside[0], separateRawCount[0], jdbcCountAfterCommit, jpaPid[0], jdbcPid[0]);
    }

    private static int countViaJdbc(JdbcTemplate jdbcTemplate) {
        // EclipseLink DDL generation creates the table as a delimited (case-sensitive) lowercase identifier
        // "visibility_probe"; quoting it here matches that exactly regardless of the database's identifier folding.
        Integer count = jdbcTemplate.queryForObject("select count(*) from \"visibility_probe\"", Integer.class);
        return count == null ? 0 : count;
    }

    // A connection taken directly from the pool, completely outside Spring's transaction synchronization, so it can
    // never be the transaction's connection. Under READ_COMMITTED it sees only committed rows.
    private static int countViaSeparateRawConnection(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select count(*) from \"visibility_probe\"")) {
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Separate-connection probe failed", e);
        }
    }

    private Fixture createFixture(boolean bindDataSourceOnManager) {
        DataSource dataSource = createDataSource();

        LocalContainerEntityManagerFactoryBean emfBean = new LocalContainerEntityManagerFactoryBean();
        emfBean.setDataSource(dataSource);
        emfBean.setPersistenceUnitName("visibility-pu-" + DB_COUNTER.get());
        emfBean.setPackagesToScan(VisibilityProbe.class.getPackageName());
        emfBean.setJpaVendorAdapter(new EclipseLinkJpaVendorAdapter());
        emfBean.setJpaPropertyMap(eclipseLinkProperties());
        emfBean.afterPropertiesSet();
        createdFactories.add(emfBean);

        ExtendedJpaTransactionManager transactionManager = new ExtendedJpaTransactionManager(false);
        transactionManager.setEntityManagerFactory(emfBean.getObject());
        if (bindDataSourceOnManager) {
            transactionManager.setDataSource(dataSource);
        }
        transactionManager.afterPropertiesSet();

        return new Fixture(dataSource, emfBean, new TransactionTemplate(transactionManager), new JdbcTemplate(dataSource));
    }

    private DataSource createDataSource() {
        DB_COUNTER.incrementAndGet();
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        // Pool size > 1 guarantees EclipseLink and JdbcTemplate can each obtain a distinct connection when not sharing.
        dataSource.setMaximumPoolSize(5);
        createdDataSources.add(dataSource);
        return dataSource;
    }

    private static Map<String, Object> eclipseLinkProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("eclipselink.weaving", "false");
        // drop-and-create keeps each test run isolated even though they share the same PostgreSQL database.
        properties.put("eclipselink.ddl-generation", "drop-and-create-tables");
        properties.put("eclipselink.ddl-generation.output-mode", "database");
        properties.put("eclipselink.target-database", "org.eclipse.persistence.platform.database.PostgreSQLPlatform");
        properties.put("eclipselink.logging.level", "SEVERE");
        return properties;
    }

    private record Fixture(DataSource dataSource, LocalContainerEntityManagerFactoryBean entityManagerFactoryBean,
            TransactionTemplate transactionTemplate, JdbcTemplate jdbcTemplate) {
    }

    private record ScenarioResult(long jpaCountInsideTransaction, int jdbcCountInsideTransaction,
            int separateRawConnectionCountInsideTransaction, int jdbcCountAfterCommit, long jpaBackendPid, long jdbcBackendPid) {
    }
}
