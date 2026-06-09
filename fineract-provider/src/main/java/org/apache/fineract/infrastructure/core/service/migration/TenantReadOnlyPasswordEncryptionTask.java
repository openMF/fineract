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
package org.apache.fineract.infrastructure.core.service.migration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import liquibase.change.custom.CustomTaskChange;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.CustomChangeException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import org.apache.fineract.infrastructure.core.service.database.DatabasePasswordEncryptor;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TenantReadOnlyPasswordEncryptionTask implements CustomTaskChange, ApplicationContextAware {

    private static DatabasePasswordEncryptor databasePasswordEncryptor;

    private static final String SELECT_READ_ONLY_PASSWORDS = """
            SELECT id, readonly_schema_password
            FROM tenant_server_connections
            WHERE readonly_schema_password IS NOT NULL
              AND readonly_schema_password <> ''
              AND (master_password_hash IS NULL OR master_password_hash = '')
            """;

    private static final String UPDATE_READ_ONLY_PASSWORD = """
            UPDATE tenant_server_connections
            SET readonly_schema_password = ?,
                master_password_hash = ?
            WHERE id = ?
              AND readonly_schema_password IS NOT NULL
              AND readonly_schema_password <> ''
            """;

    @Override
    public void execute(Database database) throws CustomChangeException {
        if (databasePasswordEncryptor == null) {
            throw new CustomChangeException("DatabasePasswordEncryptor is not initialized");
        }

        try {
            DatabaseConnection connection = database.getConnection();

            if (!(connection instanceof JdbcConnection dbConn)) {
                throw new CustomChangeException(
                        "Expected Liquibase JdbcConnection but got " + (connection == null ? "null" : connection.getClass().getName()));
            }

            try (PreparedStatement selectStatement = dbConn.prepareStatement(SELECT_READ_ONLY_PASSWORDS);
                    PreparedStatement updateStatement = dbConn.prepareStatement(UPDATE_READ_ONLY_PASSWORD);
                    ResultSet rs = selectStatement.executeQuery()) {

                String masterPasswordHash = databasePasswordEncryptor.getMasterPasswordHash();

                while (rs.next()) {
                    long id = rs.getLong("id");
                    String readOnlySchemaPassword = rs.getString("readonly_schema_password");
                    String encryptedPassword = databasePasswordEncryptor.encrypt(readOnlySchemaPassword);

                    updateStatement.setString(1, encryptedPassword);
                    updateStatement.setString(2, masterPasswordHash);
                    updateStatement.setLong(3, id);
                    updateStatement.executeUpdate();
                }
            }
        } catch (CustomChangeException e) {
            throw e;
        } catch (SQLException e) {
            throw new CustomChangeException("Failed to encrypt tenant read-only database passwords", e);
        } catch (Exception e) {
            throw new CustomChangeException("Unexpected error while encrypting tenant read-only database passwords", e);
        }
    }

    @Override
    public String getConfirmationMessage() {
        return "Tenant read-only database passwords encrypted";
    }

    @Override
    public void setUp() throws SetupException {
        // Not required
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
        // Not required
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();

        if (databasePasswordEncryptor == null) {
            validationErrors.addError("DatabasePasswordEncryptor is not initialized");
        }

        return validationErrors;
    }

    @Override
    @SuppressWarnings("static-access")
    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        TenantReadOnlyPasswordEncryptionTask.databasePasswordEncryptor = applicationContext.getBean(DatabasePasswordEncryptor.class);
    }
}
