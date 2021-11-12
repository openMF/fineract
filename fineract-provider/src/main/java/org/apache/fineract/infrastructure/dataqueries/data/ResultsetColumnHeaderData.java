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
package org.apache.fineract.infrastructure.dataqueries.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.fineract.infrastructure.core.boot.db.DataSourceDialect;
import org.apache.fineract.infrastructure.core.boot.db.JdbcJavaType;
import org.apache.fineract.infrastructure.core.exception.PlatformDataIntegrityException;

import javax.validation.constraints.NotNull;

/**
 * Immutable data object representing a resultset column.
 */
public final class ResultsetColumnHeaderData implements Serializable {

    private final String columnName;
    private final JdbcJavaType columnType;
    private final Long columnLength;
    private final DisplayType columnDisplayType;
    private final boolean isColumnNullable;
    @SuppressWarnings("unused")
    private final boolean isColumnPrimaryKey;

    private final List<ResultsetColumnValueData> columnValues;
    private final String columnCode;


    public static ResultsetColumnHeaderData basic(final String columnName, final String columnType, DataSourceDialect dialect) {
        final Long columnLength = null;
        final boolean columnNullable = false;
        final boolean columnIsPrimaryKey = false;
        final List<ResultsetColumnValueData> columnValues = new ArrayList<>();
        final String columnCode = null;
        return new ResultsetColumnHeaderData(columnName, columnType, columnLength, columnNullable, columnIsPrimaryKey, columnValues,
                columnCode, dialect);
    }

    public static ResultsetColumnHeaderData detailed(final String columnName, final String columnType, final Long columnLength,
                                                     final boolean columnNullable, final boolean columnIsPrimaryKey, final List<ResultsetColumnValueData> columnValues,
                                                     final String columnCode, DataSourceDialect dialect) {
        return new ResultsetColumnHeaderData(columnName, columnType, columnLength, columnNullable, columnIsPrimaryKey, columnValues,
                columnCode, dialect);
    }

    private ResultsetColumnHeaderData(final String columnName, String columnType, final Long columnLength,
                                      final boolean columnNullable, final boolean columnIsPrimaryKey, final List<ResultsetColumnValueData> columnValues,
                                      final String columnCode, DataSourceDialect dialect) {
        this.columnName = columnName;
        this.columnLength = columnLength;
        this.isColumnNullable = columnNullable;
        this.isColumnPrimaryKey = columnIsPrimaryKey;
        this.columnValues = columnValues;
        this.columnCode = columnCode;

        // Refer org.drizzle.jdbc.internal.mysql.MySQLType.java
        columnType = adjustColumnTypes(columnType);
        this.columnType = JdbcJavaType.getByTypeName(dialect, columnType);

        this.columnDisplayType = calcDisplayType();
    }

    private DisplayType calcDisplayType() {
        if (this.columnCode == null) {
            DisplayType displayType = calcColumnDisplayType(columnType);
            if (displayType != null) {
                return displayType;
            }

            throw new PlatformDataIntegrityException("error.msg.invalid.lookup.type", "Invalid Lookup Type:" + this.columnType
                    + " - Column Name: " + this.columnName);
        } else {
            if (columnType.isIntegerType())
                return DisplayType.CODELOOKUP;
            if (columnType.isVarcharType())
                return DisplayType.CODEVALUE;

            throw new PlatformDataIntegrityException("error.msg.invalid.lookup.type", "Invalid Lookup Type:" + this.columnType
                    + " - Column Name: " + this.columnName);
        }
    }

    public static DisplayType calcColumnDisplayType(JdbcJavaType columnType) {
        if (columnType.isTextType())
            return DisplayType.TEXT;
        if (columnType.isStringType())
            return DisplayType.STRING;
        if (columnType.isAnyIntegerType())
            return DisplayType.INTEGER;
        if (columnType.isAnyFloatType())
            return DisplayType.FLOAT;
        if (columnType.isDecimalType()) // Refer org.drizzle.jdbc.internal.mysql.MySQLType.java
            return DisplayType.DECIMAL;
        if (columnType.isDateType())
            return DisplayType.DATE;
        if (columnType.isDateTimeType())
            return DisplayType.DATETIME;
        if (columnType.isTimeType())
            return DisplayType.TIME;
        if (columnType.isBooleanType())
            return DisplayType.BOOLEAN;
        if (columnType.isBinaryType())
            return DisplayType.BINARY;
        return null;
    }

    public enum DisplayType {
        TEXT,
        STRING,
        INTEGER,
        FLOAT,
        DECIMAL,
        DATE,
        TIME,
        DATETIME,
        BOOLEAN,
        BINARY,
        CODELOOKUP,
        CODEVALUE,
        ;
    }

    private String adjustColumnTypes(String type) {
        type = type.toUpperCase();
        switch (type) {
            case "CLOB":
            case "ENUM":
            case "SET":
                return "VARCHAR";
            case "NEWDECIMAL":
                return "DECIMAL";
            case "LONGLONG":
                return "BIGINT";
            case "SHORT":
                return "SMALLINT";
            case "TINY":
                return "TINYINT";
            case "TIMESTAMP WITHOUT TIME ZONE":
                return "TIMESTAMP";
            default:
                return type;
        }
    }

    public String getColumnName() {
        return this.columnName;
    }

    public boolean isNamed(final String columnName) {
        return this.columnName.equalsIgnoreCase(columnName);
    }

    public JdbcJavaType getColumnType() {
        return this.columnType;
    }

    public Long getColumnLength() {
        return this.columnLength;
    }

    public String getColumnCode() {
        return this.columnCode;
    }

    public boolean isColumnNullable() {
        return isColumnNullable;
    }

    public DisplayType getColumnDisplayType() {
        return this.columnDisplayType;
    }

    public boolean isDateDisplayType() {
        return columnDisplayType == DisplayType.DATE;
    }

    public boolean isDateTimeDisplayType() {
        return columnDisplayType == DisplayType.DATETIME;
    }

    public boolean isTimeDisplayType() {
        return columnDisplayType == DisplayType.TIME;
    }

    public boolean isIntegerDisplayType() {
        return columnDisplayType == DisplayType.INTEGER;
    }

    public boolean isDecimalDisplayType() {
        return columnDisplayType == DisplayType.DECIMAL;
    }

    public boolean isStringDisplayType() {
        return columnDisplayType == DisplayType.STRING;
    }

    public boolean isTextDisplayType() {
        return columnDisplayType == DisplayType.TEXT;
    }

    public boolean isBooleanDisplayType() {
        return columnDisplayType == DisplayType.BOOLEAN;
    }

    public boolean isCodeValueDisplayType() {
        return columnDisplayType == DisplayType.CODEVALUE;
    }

    public boolean isCodeLookupDisplayType() {
        return columnDisplayType == DisplayType.CODELOOKUP;
    }

    public boolean hasPrecision(@NotNull DataSourceDialect dialect) {
        return columnType.hasPrecision(dialect);
    }

    public boolean hasColumnValues() {
        return !this.columnValues.isEmpty();
    }

    public boolean isColumnValueAllowed(final String match) {
        if (match == null)
            return false;
        for (final ResultsetColumnValueData allowedValue : this.columnValues) {
            if (allowedValue.matches(match))
                return true;
        }
        return false;
    }

    public boolean isColumnCodeAllowed(final Integer match) {
        if (match == null)
            return false;
        for (final ResultsetColumnValueData allowedValue : this.columnValues) {
            if (allowedValue.codeMatches(match))
                return true;
        }
        return false;
    }
}
