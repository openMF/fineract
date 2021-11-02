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

import org.apache.fineract.infrastructure.core.boot.JDBCDriverConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_ADD_COLUMN;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_ADD_FK;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_ADD_INDEX;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_ADD_UK;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_ALTER;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_ALTER_COLUMN;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_CREATE;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_CREATE_COLUMN;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_CREATE_FK;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_CREATE_PK;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_CREATE_SETTINGS;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_CREATE_UK;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_DROP_COLUMN;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_DROP_FK;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_DROP_INDEX;
import static org.apache.fineract.infrastructure.core.boot.db.SqlDefinition.TABLE_DROP_UK;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.CAST;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.CURDATE;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.DATE;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.DATEADD;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.DATEDIFF;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.DATESUB;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.GROUP_CONCAT;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.LAST_INSERT_ID;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.SCHEMA;
import static org.apache.fineract.infrastructure.core.boot.db.SqlFunction.TIME;

@Component
public class DataSourceSqlResolver {

    private static final Object NO_DEF_VALUE = new Object();

    private JDBCDriverConfig dataSourceConfig;

    @Autowired
    public DataSourceSqlResolver(JDBCDriverConfig dataSourceConfig) {
        this.dataSourceConfig = dataSourceConfig;
    }

    public DataSourceDialect getDialect() {
        return dataSourceConfig.getDialect();
    }

    // --- functions ---

    public String formatDate(@NotNull String date) {
        return DATE.formatSql(getDialect(), date);
    }

    public String formatTime(@NotNull String date) {
        return TIME.formatSql(getDialect(), date);
    }

    public String formatDateCurrent() {
        return CURDATE.formatSql(getDialect());
    }

    /**
     * NOTE: mysql DATE_ADD handles INTERVAL only (while Postgres is able to handle any units), therefore this method adds intervals only to the start date
     * and handles one unit only.
     */
    public String formatDateAdd(@NotNull String date, @NotNull DateUnit unit) {
        return DATEADD.formatSql(getDialect(), date, "1", unit.name());
    }

    /**
     * NOTE: mysql DATE_ADD handles INTERVAL only (while Postgres is able to handle any units), therefore this method adds intervals only to the start date
     * and handles one unit only.
     * @param multiplier is an integer, but can be a field name or parameter mark so we allow any string
     */
    public String formatDateAdd(@NotNull String date, @NotNull String multiplier, @NotNull DateUnit unit) {
        return DATEADD.formatSql(getDialect(), date, multiplier, unit.name());
    }

    /**
     * NOTE: mysql DATE_SUB handles INTERVAL only (while Postgres is able to handle any units), therefore this method subtracts intervals only from the start date
     * and handles one unit only.
     */
    public String formatDateSub(@NotNull String date, @NotNull DateUnit unit) {
        return DATESUB.formatSql(getDialect(), date, "1", unit.name());
    }

    /**
     * NOTE: mysql DATE_SUB handles INTERVAL only (while Postgres is able to handle any units), therefore this method subtracts intervals only from the start date
     * and handles one unit only.
     * @param multiplier is an integer, but can be a field name or parameter mark so we allow any string
     */
    public String formatDateSub(@NotNull String date, @NotNull String multiplier, @NotNull DateUnit unit) {
        return DATESUB.formatSql(getDialect(), date, multiplier, unit.name());
    }

    public String formatDateDiff(@NotNull String date1, @NotNull String date2) {
        return DATEDIFF.formatSql(getDialect(), date1, date2);
    }

    public String formatCast(@NotNull String value, @NotNull String type) {
        return CAST.formatSql(getDialect(), value, type);
    }

    public String formatSchema() {
        return SCHEMA.formatSql(getDialect());
    }

    public String formatLastInsertId() {
        return LAST_INSERT_ID.formatSql(getDialect());
    }

    public String formatGroupConcat(@NotNull String column) {
        return GROUP_CONCAT.formatSql(getDialect(), column);
    }

    // --- definitions ---

    public StringBuilder formatCreateTable(@NotNull StringBuilder sql, @NotNull String table) {
        TABLE_CREATE.formatSql(getDialect(), sql, false, toDefinition(table));
        return sql;
    }

    public StringBuilder formatAlterTable(@NotNull StringBuilder sql, @NotNull String table) {
        TABLE_ALTER.formatSql(getDialect(), sql, false, toDefinition(table));
        return sql;
    }

    public StringBuilder formatCreateTableSettings(@NotNull StringBuilder sql, boolean embedded) {
        TABLE_CREATE_SETTINGS.formatSql(getDialect(), sql, embedded);
        return sql;
    }

    public StringBuilder formatCreateTablePk(@NotNull StringBuilder sql, @NotNull String column) {
        TABLE_CREATE_PK.formatSql(getDialect(), sql, true, toDefinition(column));
        return sql;
    }

    public StringBuilder formatCreateTableFk(@NotNull StringBuilder sql, @NotNull String name, @NotNull String column, @NotNull String refdTable, @NotNull String refdColumn) {
        TABLE_CREATE_FK.formatSql(getDialect(), sql, true, toDefinition(name), toDefinition(column), toDefinition(refdTable), toDefinition(refdColumn));
        return sql;
    }

    public StringBuilder formatCreateTableFk(@NotNull StringBuilder sql, @NotNull String name, @NotNull String[] columns, @NotNull String refdTable, @NotNull String[] refdColumns) {
        String columnsString = Arrays.stream(columns).map(this::toDefinition).collect(Collectors.joining(", "));
        String refdColumnsString = Arrays.stream(refdColumns).map(this::toDefinition).collect(Collectors.joining(", "));
        TABLE_CREATE_FK.formatSql(getDialect(), sql, true, toDefinition(name), columnsString, toDefinition(refdTable), refdColumnsString);
        return sql;
    }

    public StringBuilder formatAddTableFk(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull String column, @NotNull String refdTable, @NotNull String refdColumn) {
        TABLE_ADD_FK.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name), toDefinition(column), toDefinition(refdTable),
                toDefinition(refdColumn));
        return sql;
    }

    public StringBuilder formatAddTableFk(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull String[] columns, @NotNull String refdTable, @NotNull String[] refdColumns) {
        String columnsString = Arrays.stream(columns).map(this::toDefinition).collect(Collectors.joining(", "));
        String refdColumnsString = Arrays.stream(refdColumns).map(this::toDefinition).collect(Collectors.joining(", "));
        TABLE_ADD_FK.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name), columnsString, toDefinition(refdTable),
                refdColumnsString);
        return sql;
    }

    public StringBuilder formatDropTableFk(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name) {
        TABLE_DROP_FK.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name));
        return sql;
    }

    public StringBuilder formatCreateTableUk(@NotNull StringBuilder sql, @NotNull String name, @NotNull String column) {
        TABLE_CREATE_UK.formatSql(getDialect(), sql, true, toDefinition(name), toDefinition(column));
        return sql;
    }

    public StringBuilder formatCreateTableUk(@NotNull StringBuilder sql, @NotNull String name, @NotNull String[] columns) {
        String columnsString = Arrays.stream(columns).map(this::toDefinition).collect(Collectors.joining(", "));
        TABLE_CREATE_UK.formatSql(getDialect(), sql, true, toDefinition(name), columnsString);
        return sql;
    }

    public StringBuilder formatAddTableUk(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull String column) {
        TABLE_ADD_UK.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name), toDefinition(column));
        return sql;
    }

    public StringBuilder formatAddTableUk(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull String[] columns) {
        String columnsString = Arrays.stream(columns).map(this::toDefinition).collect(Collectors.joining(", "));
        TABLE_ADD_UK.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name), columnsString);
        return sql;
    }

    public StringBuilder formatDropTableUk(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name) {
        TABLE_DROP_UK.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name));
        return sql;
    }

    public StringBuilder formatAddTableIndex(@NotNull StringBuilder sql, @NotNull String table, @NotNull String name, @NotNull String column) {
        TABLE_ADD_INDEX.formatSql(getDialect(), sql, false, toDefinition(table), toDefinition(name), toDefinition(column));
        return sql;
    }

    public StringBuilder formatAddTableIndex(@NotNull StringBuilder sql, @NotNull String table, @NotNull String name, @NotNull String[] columns) {
        TABLE_ADD_INDEX.formatSql(getDialect(), sql, false, toDefinition(table), toDefinition(name), String.join(",", columns));
        return sql;
    }

    public StringBuilder formatDropTableIndex(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name) {
        TABLE_DROP_INDEX.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name));
        return sql;
    }

    public StringBuilder formatCreateTableColumn(@NotNull StringBuilder sql, @NotNull String name, @NotNull JdbcJavaType columnType, Integer precision, Integer scale, Boolean nullable) {
        return formatCreateTableColumn(sql, name, columnType, precision, scale, nullable, NO_DEF_VALUE);
    }

    public StringBuilder formatCreateTableColumn(@NotNull StringBuilder sql, @NotNull String name, @NotNull JdbcJavaType columnType, Integer precision, Integer scale, Boolean nullable, Object defValue) {
        TABLE_CREATE_COLUMN.formatSql(getDialect(), sql, true, toDefinition(name), formatType(columnType, precision, scale), formatNullable(columnType, nullable),
                formatDefaultValue(columnType, nullable, defValue));
        return sql;
    }

    public StringBuilder formatAddTableColumn(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull JdbcJavaType columnType, Integer precision, Integer scale, Boolean nullable) {
        return formatAddTableColumn(sql, embedded, table, name, columnType, precision, scale, nullable, NO_DEF_VALUE, null);
    }

    public StringBuilder formatAddTableColumn(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull JdbcJavaType columnType, Integer precision, Integer scale, Boolean nullable,
                                              ColumnPosition position) {
        return formatAddTableColumn(sql, embedded, table, name, columnType, precision, scale, nullable, NO_DEF_VALUE, position);
    }

    public StringBuilder formatAddTableColumn(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull JdbcJavaType columnType, Integer precision, Integer scale, Boolean nullable,
                                              Object defValue, ColumnPosition position) {
        String posString = position == null ? null : position.formatSql();
        TABLE_ADD_COLUMN.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name), formatType(columnType, precision, scale), formatNullable(columnType, nullable),
                formatDefaultValue(columnType, nullable, defValue), posString);
        return sql;
    }

    public StringBuilder formatAlterTableColumn(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull String oldName, @NotNull JdbcJavaType columnType, Integer precision,
                                                Integer scale, Boolean nullable, ColumnPosition position) {
        return formatAlterTableColumn(sql, embedded, table, name, oldName, columnType, precision, scale, nullable, NO_DEF_VALUE, position);
    }

    public StringBuilder formatAlterTableColumn(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name, @NotNull String oldName, @NotNull JdbcJavaType columnType, Integer precision,
                                                Integer scale, Boolean nullable, Object defValue, ColumnPosition position) {
        String posString = position == null ? null : position.formatSql();
        TABLE_ALTER_COLUMN.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name), toDefinition(oldName), formatType(columnType, precision, scale),
                formatNullable(columnType, nullable), formatDefaultValue(columnType, nullable, defValue), posString);
        return sql;
    }

    public StringBuilder formatDropTableColumn(@NotNull StringBuilder sql, boolean embedded, @NotNull String table, @NotNull String name) {
        TABLE_DROP_COLUMN.formatSql(getDialect(), sql, embedded, toDefinition(table), toDefinition(name));
        return sql;
    }

    // ---- Column types and values ----

    public String formatBoolValue(Boolean value) {
        return formatJdbcValue(JdbcJavaType.BOOLEAN, value);
    }

    public String formatJdbcValue(JdbcJavaType columnType, Object value) {
        return String.valueOf(toJdbcValue(columnType, value));
    }

    public Object toBoolValue(Boolean value) {
        return toJdbcValue(JdbcJavaType.BOOLEAN, value);
    }

    public Object toJdbcValue(@NotNull JdbcJavaType columnType, Object value) {
        return columnType.toJdbcValue(getDialect(), value);
    }

    public String formatType(@NotNull JdbcJavaType columnType) {
        return columnType.formatSql(getDialect());
    }

    public String formatType(@NotNull JdbcJavaType columnType, Integer precision) {
        return columnType.formatSql(getDialect(), precision);
    }

    public String formatType(@NotNull JdbcJavaType columnType, Integer precision, Integer scale) {
        return columnType.formatSql(getDialect(), precision, scale);
    }

    @NotNull
    public String formatNullable(@NotNull JdbcJavaType columnType, Boolean nullable) {
        return nullable == null ? "" : (nullable ? "NULL" : "NOT NULL");
    }

    @NotNull
    public String formatDefaultValue(@NotNull JdbcJavaType columnType, Boolean nullable, Object defValue) {
        if (defValue == NO_DEF_VALUE)
            return "";

        if (defValue == null) {
            if (nullable)
                return "NULL";
        }
        else
            return formatJdbcValue(columnType, defValue);
        return null;
    }

    public String toDefinition(String dbName) {
        return toDefinition(getDialect(), dbName);
    }

    public static String toDefinition(DataSourceDialect dialect, String dbName) {
        if (dbName == null)
            return null;
        char esc = getDefinitionChar(dialect);
        return esc + dbName + esc;
    }

    public char getDefinitionChar() {
        return getDefinitionChar(getDialect());
    }

    public static char getDefinitionChar(DataSourceDialect dialect) {
        return dialect.isMySql() ? '`' : '"';
    }

    public ColumnPosition columnPositionAfter(String after) {
        return after == null ? null : ColumnPosition.after(getDialect(), after);
    }

    public enum ColumnPositionType {
        FIRST,
        AFTER,
        LAST,
        ;
    }

    public static class ColumnPosition {
        private final ColumnPositionType type;
        private final String after;

        public ColumnPosition(@NotNull DataSourceDialect dialect, @NotNull ColumnPositionType type, String after) {
            this.type = type;
            this.after = toDefinition(dialect, after);
        }

        public String formatSql() {
            switch (type) {
                case LAST:
                    return "";
                case FIRST:
                    return type.name();
                case AFTER:
                    return type.name() + ' ' + after;
            }
            return null;
        }

        public static ColumnPosition after(@NotNull DataSourceDialect dialect, @NotNull String after) {
            return new ColumnPosition(dialect, ColumnPositionType.AFTER, after);
        }
    }

    public enum DateUnit {
        MICROSECOND,
        MILLISECOND,
        SECOND,
        MINUTE,
        HOUR,
        DAY,
        WEEK,
        MONTH,
        QUARTER,
        YEAR
    }
}