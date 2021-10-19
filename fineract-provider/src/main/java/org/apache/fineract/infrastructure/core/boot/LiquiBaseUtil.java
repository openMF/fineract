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
package org.apache.fineract.infrastructure.core.boot;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.configuration.GlobalConfiguration;
import liquibase.configuration.LiquibaseConfiguration;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.lockservice.LockServiceFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiquiBaseUtil {

    private static final Logger LOG = LoggerFactory.getLogger(LiquiBaseUtil.class);

    private static Long liquibaseLockThreshold = 0L;

    private LiquiBaseUtil() {}

    public static void applyLiquibaseMigrations(String changeLog, Connection connection) {
        Liquibase liquibase = null;
        try {
            // Get database connection
            JdbcConnection jdbcCon = new JdbcConnection(connection);

            // Overwrite default liquibase table names by custom
            GlobalConfiguration liquibaseConfiguration = LiquibaseConfiguration.getInstance().getConfiguration(GlobalConfiguration.class);
            liquibaseConfiguration.setDatabaseChangeLogLockWaitTime(1L);

            // Initialize Liquibase and run the update
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(jdbcCon);
            LOG.info("database: " + database.getDatabaseProductName() + " - " + database.getDatabaseProductVersion());
            LOG.info("changeLog: " + changeLog);
            ClassLoader classLoader = LiquiBaseUtil.class.getClassLoader();
            ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor(classLoader);
            liquibase = new Liquibase(changeLog, resourceAccessor, database);

            boolean liquibaseExecuted = false;
            while (!liquibaseExecuted) {
                try {
                    liquibase.update(new Contexts(), new LabelExpression());
                    liquibaseExecuted = true;
                } catch (LockException ex) {
                    LOG.warn("LiquiBaseUtil applyLiquibaseMigration() getting LockException ", ex);
                    releaseLiquibaseLock(connection);
                }
            }
        } catch (DatabaseException e) {
            LOG.error(e.getMessage(), e);
        } catch (LiquibaseException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (liquibase != null) {
                try {
                    liquibase.close();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    private static void releaseLiquibaseLock(Connection connection) {
        // Get database connection
        try {
            boolean existsStatus = tableExists(connection, "database_change_log_lock");
            if (!existsStatus) {
                LOG.info("Table database_change_log_lock does not exists in DB");
                LOG.info("Proceeding with liquibase assuming it has never been run");
                return;
            }
            JdbcConnection jdbcCon = new JdbcConnection(connection);

            Statement stmt = jdbcCon.createStatement();

            String sql = "SELECT * FROM database_change_log_lock WHERE ID = 1";
            ResultSet rs = stmt.executeQuery(sql);

            long lastLockAcquireTimestamp = 0L;
            boolean locked = false;
            // Extract data from result set
            while (rs.next()) {
                // Retrieve by column name
                int id = rs.getInt("id");
                locked = rs.getBoolean("locked");
                Timestamp lockGrantedTimeStamp = rs.getTimestamp("lockgranted", Calendar.getInstance());
                String lockedBy = rs.getString("lockedby");

                // Display values
                LOG.debug("Id: {}, Locked: {}, LockGrantedTimeStamp: {}, LockedBy: {}", id, locked, lockGrantedTimeStamp, lockedBy);

                if (lockGrantedTimeStamp != null) {
                    lastLockAcquireTimestamp = lockGrantedTimeStamp.getTime();
                }
                LOG.debug("database locked by Liquibase: {}", locked);
            }
            rs.close();
            stmt.close();

            Calendar currentCalender = Calendar.getInstance();
            long currentLockedTimeDiffSecond = (currentCalender.getTimeInMillis() - lastLockAcquireTimestamp) / 1000;
            LOG.debug("current liquibase locked time difference in second: {}", currentLockedTimeDiffSecond);
            if (lastLockAcquireTimestamp != 0 && currentLockedTimeDiffSecond > liquibaseLockThreshold) {
                // Initialize Liquibase and run the update
                Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(jdbcCon);
                LockServiceFactory.getInstance().getLockService(database).forceReleaseLock();
                locked = false;
                LOG.debug("Release database lock executing query from backend");
            }

            if (locked) {
                Thread.sleep(liquibaseLockThreshold * 1000); // liquibaseLockThreshold = second
                releaseLiquibaseLock(connection);
            }
        } catch (LockException e) {
            LOG.error(e.getMessage(), e);
        } catch (DatabaseException e) {
            LOG.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static boolean tableExists(Connection connection, String tableName) {
        boolean tExists = false;
        try {
            ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null);
            while (rs.next()) {
                String tName = rs.getString("TABLE_NAME");
                if (tName != null && tName.equals(tableName)) {
                    tExists = true;
                    break;
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        return tExists;
    }

}
