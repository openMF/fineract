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
package org.apache.fineract.infrastructure.core.boot.db;

import org.apache.fineract.infrastructure.core.exception.PlatformServiceUnavailableException;
import org.apache.logging.log4j.util.Strings;

import javax.validation.constraints.NotNull;
import java.util.Arrays;

public enum SqlDefinition {

    // Mysql, PostgreSql
    TABLE_CREATE() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 1, params);
            sql.append("CREATE TABLE IF NOT EXISTS ").append(params[0]);
        }
    },
    TABLE_ALTER() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 1, params);
            sql.append("ALTER TABLE ").append(params[0]);
        }
    },
    TABLE_CREATE_SETTINGS() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 0, params);
            if (dialect.isMySql()) {
                sql.append("ENGINE=InnoDB DEFAULT CHARSET=utf8");
                if (!embedded)
                    sql.append(';');
            }
        }
    },
    TABLE_CREATE_PK() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            formatSql(dialect, sql, embedded, "PRIMARY KEY", 1, params);
        }
    },
    TABLE_CREATE_FK() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 4, params);
            sql.append("CONSTRAINT ").append(params[0]).append(" FOREIGN KEY (").append(params[1]).append(") REFERENCES ").append(params[2]).append(" (").append(params[3])
                    .append(')');
        }
    },
    TABLE_ADD_FK() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 5, params);

            if (!embedded)
                sql.append("ALTER TABLE ").append(params[0]).append(' ');

            sql.append("ADD CONSTRAINT ").append(params[1]).append(" FOREIGN KEY (").append(params[2]).append(") REFERENCES ").append(params[3]).append(" (").append(params[4])
                    .append(')');
            if (!embedded)
                sql.append(';');
        }
    },
    TABLE_DROP_FK() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 2, params);

            if (!embedded)
                sql.append("ALTER TABLE ").append(params[0]).append(' ');

            sql.append("DROP CONSTRAINT IF EXISTS ").append(params[1]);
            if (!embedded)
                sql.append(';');
        }
    },
    TABLE_CREATE_UK() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 2, params);
            sql.append("CONSTRAINT ").append(params[0]).append(" UNIQUE (").append(params[1]).append(')');
        }
    },
    TABLE_ADD_UK() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 2, params);

            if (!embedded)
                sql.append("ALTER TABLE ").append(params[0]).append(' ');

            sql.append("ADD CONSTRAINT ").append(params[1]).append(" UNIQUE (").append(params[2]).append(')');
            if (!embedded)
                sql.append(';');
        }
    },
    TABLE_DROP_UK() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 2, params);

            if (!embedded)
                sql.append("ALTER TABLE ").append(params[0]).append(' ');

            sql.append("DROP CONSTRAINT IF EXISTS ").append(params[1]);
            if (!embedded)
                sql.append(';');
        }
    },
    TABLE_ADD_INDEX() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 3, params);
            sql.append("CREATE INDEX ").append(params[1]).append(" ON ").append(params[0]).append(" (").append(params[2]).append(')');
            if (!embedded)
                sql.append(';');
        }
    },
    TABLE_DROP_INDEX() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 2, params);

            if (!embedded)
                sql.append("ALTER TABLE ").append(params[0]).append(' ');

            sql.append("DROP CONSTRAINT IF EXISTS ").append(params[1]);
            if (!embedded)
                sql.append(';');
        }
    },
    TABLE_CREATE_COLUMN() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 4, params);

            String name = params[0];
            String type = params[1];
            String nullable = params[2];
            String defValue = params[3];

            sql.append(name).append(' ').append(type);
            if (!Strings.isEmpty(nullable))
                sql.append(' ').append(nullable);
            if (!Strings.isEmpty(defValue))
                sql.append(" DEFAULT ").append(defValue);
        }
    },
    TABLE_ADD_COLUMN() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 6, params);

            String table = params[0];
            String name = params[1];
            String type = params[2];
            String nullable = params[3];
            String defValue = params[4];
            String position = params[5];

            if (!embedded)
                sql.append("ALTER TABLE ").append(table).append(' ');

            sql.append("ADD COLUMN ").append(name).append(' ').append(type);
            if (!Strings.isEmpty(nullable))
                sql.append(' ').append(nullable);
            if (!Strings.isEmpty(defValue))
                sql.append(" DEFAULT ").append(defValue);
            if (!Strings.isEmpty(position) && dialect.isMySql())
                sql.append(' ').append(position);
            if (!embedded)
                sql.append(';');
        }
    },
    TABLE_ALTER_COLUMN() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 7, params);

            String table = params[0];
            String name = params[1];
            String oldName = params[2];
            String type = params[3];
            String nullable = params[4];
            String defValue = params[5];
            String position = params[6];

            boolean actEmbed = embedded;
            String alterTable = "ALTER TABLE " + table + ' ';

            if (oldName != null && !oldName.equals(name)) {
                if (actEmbed && replaceAtEnd(sql, ',', ';')) {
                    sql.append(' ').append(alterTable);
                }
                sql.append("RENAME COLUMN ").append(oldName).append(" TO ").append(name).append("; ");
                actEmbed = false;
            }

            boolean changed = false;
            if (dialect.isMySql()) {
                if (defValue != null && defValue.isEmpty()) {
                    if (actEmbed && replaceAtEnd(sql, ',', ';')) {
                        sql.append(' ').append(alterTable);
                    }
                    sql.append("DROP DEFAULT; ");
                    actEmbed = false;
                }

                String modifyColumn = "MODIFY COLUMN " + name + ' ';
                if (!Strings.isEmpty(type)) {
                    sql.append((actEmbed ? "" : alterTable)).append(modifyColumn).append(type);
                    actEmbed = true;
                    changed = true;
                }
                if (!Strings.isEmpty(nullable)) {
                    sql.append(changed ? ' ' : (actEmbed ? "" : alterTable)).append(modifyColumn).append(nullable);
                    actEmbed = true;
                    changed = true;
                }
                if (!Strings.isEmpty(defValue)) {
                    sql.append(changed ? ' ' : (actEmbed ? "" : alterTable)).append(modifyColumn).append("DEFAULT ").append(defValue);
                    actEmbed = true;
                    changed = true;
                }
                if (!Strings.isEmpty(position)) {
                    sql.append(changed ? ' ' : (actEmbed ? "" : alterTable)).append(modifyColumn).append(position);
                    actEmbed = true;
                    changed = true;
                }
            }
            else {
                String modifyColumn = "ALTER " + name + ' ';
                if (defValue != null) {
                    sql.append(actEmbed ? "" : alterTable).append(modifyColumn).append(defValue.isEmpty() ? "DROP DEFAULT" : "SET DEFAULT ").append(defValue);
                    actEmbed = true;
                    changed = true;
                }
                if (!Strings.isEmpty(type)) {
                    sql.append(changed ? ", " : (actEmbed ? "" : alterTable)).append(modifyColumn).append("TYPE ").append(type);
                    actEmbed = true;
                    changed = true;
                }
                if (!Strings.isEmpty(nullable)) {
                    sql.append(changed ? ", " : (actEmbed ? "" : alterTable)).append(modifyColumn).append(("NULL".equalsIgnoreCase(nullable) ? "DROP NOT NULL" : "SET NOT NULL"));
                    actEmbed = true;
                    changed = true;
                }
                // not supported to set position
            }
            if (embedded != actEmbed)
                sql.append(embedded ? alterTable : ";");
        }
    },
    TABLE_DROP_COLUMN() {
        @Override
        public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
            validateParams(dialect, embedded, 2, params);

            if (!embedded)
                sql.append("ALTER TABLE ").append(params[0]).append(' ');

            sql.append("DROP COLUMN ").append(params[1]);
            if (!embedded)
                sql.append(';');
        }
    },
    ;

    private static boolean replaceAtEnd(@NotNull StringBuilder sql, char from, char to) {
        int idx = sql.length();
        while (--idx >= 0) {
            char c = sql.charAt(idx);
            if (c == from) {
                sql.setCharAt(idx, to);
                return true;
            }
            if (!Character.isWhitespace(c))
                break;
        }
        return false;
    }

    SqlDefinition() {
    }

    public void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String... params) {
        formatSql(dialect, sql, embedded, params == null ? 0 : params.length, params);
    }

    protected void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, int paramCount, String... params) {
        formatSql(dialect, sql, embedded, name(), paramCount, params);
    }

    protected void formatSql(@NotNull DataSourceDialect dialect, @NotNull StringBuilder sql, boolean embedded, String definition, int paramCount, String... params) {
        validateParams(dialect, embedded, paramCount, params);
        String paramsString = params == null ? "" : String.join(",", params);
        sql.append(definition.toUpperCase()).append(" (").append(paramsString).append(')');
        if (!embedded)
            sql.append(';');
    }

    protected void validateParams(@NotNull DataSourceDialect dialect, boolean embedded, int paramCount, String... params) {
        if (params == null ? paramCount != 0 : params.length != paramCount)
            throw new PlatformServiceUnavailableException("error.msg.database.definition.invalid", "Number of parameters " + Arrays.toString(params) +
                    " must be " + paramCount + " on " + this);
    }
}
