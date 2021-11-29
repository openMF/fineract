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

import javax.validation.constraints.NotNull;
import java.util.Arrays;

public enum SqlFunction {

    // Mysql, PostgreSql
    DATE() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            if (dialect.isMySql()) {
                return formatSql(dialect, 1, params);
            }

            validateParams(dialect, 1, params);
            return "CAST(" + params[0] + " AS DATE)";
        }
    },
    TIME() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            if (dialect.isMySql()) {
                return formatSql(dialect, 1, params);
            }

            validateParams(dialect, 1, params);
            return "CAST(" + params[0] + " AS TIME)";
        }
    },
    CURDATE() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            if (dialect.isMySql()) {
                return formatSql(dialect, 0, params);
            }

            validateParams(dialect, 0, params);
            return "CURRENT_DATE";
        }
    },
    DATEADD() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            validateParams(dialect, 3, params);

            String date = params[0];
            String multiplier = params[1];
            String unit = params[2];

            return dialect.isMySql()
                    ? ("DATE_ADD(" + date + ", INTERVAL " + multiplier + ' ' + unit + ')')
                    : ("(" + date + "::TIMESTAMP + " + multiplier + " * INTERVAL '1 " + unit + "')");
        }
    },
    DATESUB() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            validateParams(dialect, 3, params);

            String date = params[0];
            String multiplier = params[1];
            String unit = params[2];

            return dialect.isMySql()
                    ? ("DATE_SUB(" + date + ", INTERVAL " + multiplier + ' ' + unit + ')')
                    : ("(" + date + "::TIMESTAMP - " + multiplier + " * INTERVAL '1 " + unit + "')");
        }
    },
    DATEDIFF() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            if (dialect.isMySql()) {
                return formatSql(dialect, 2, params);
            }

            validateParams(dialect, 2, params);
            return "EXTRACT(DAY FROM (" + params[0] + "::TIMESTAMP - " + params[1] + "::TIMESTAMP))";
        }
    },
    DATEDIFFMONTH() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            validateParams(dialect, 2, params);
            return dialect.isMySql()
                    ? "TIMESTAMPDIFF(MONTH, " + params[1] + ", " + params[0] + ')'
                    : "(EXTRACT(YEAR FROM AGE(" + params[0] + "::TIMESTAMP - " + params[1] + "::TIMESTAMP)) * 12" +
                    " + EXTRACT(MONTH FROM AGE(" + params[0] + "::TIMESTAMP - " + params[1] + "::TIMESTAMP)))";
        }
    },
    CAST() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            validateParams(dialect, 2, params);
            String type = params[1];
            if (dialect.isMySql()) {
                if (type.equalsIgnoreCase("varchar"))
                    type = "CHAR";
                return "CAST(" + params[0] + " AS " + type + ')';
            }
            return params[0] + "::" + type;
        }
    },
    SCHEMA() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            return dialect.isMySql()
                    ? formatSql(dialect, 0, params)
                    : formatSql(dialect, "CURRENT_SCHEMA", 0, params);
        }
    },
    LAST_INSERT_ID() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            return dialect.isMySql()
                    ? formatSql(dialect, 0, params)
                    : formatSql(dialect, "LASTVAL", 0, params);
        }
    },
    GROUP_CONCAT() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            if (dialect.isMySql())
                return formatSql(dialect, 1, params);

            validateParams(dialect, 1, params);
            return " STRING_AGG(" + params[0] + ", ',') ";
        }
    },
    SUBSTRING() {
        @Override
        public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
            return formatSql(dialect, "SUBSTR", params.length > 2 ? 3 : 2, params);
        }
    },
    ;

    SqlFunction() {
    }

    public String formatSql(@NotNull DataSourceDialect dialect, String... params) {
        return formatSql(dialect, params == null ? 0 : params.length, params);
    }

    protected String formatSql(@NotNull DataSourceDialect dialect, int paramCount, String... params) {
        return formatSql(dialect, name(), paramCount, params);
    }

    protected String formatSql(@NotNull DataSourceDialect dialect, String function, int paramCount, String... params) {
        validateParams(dialect, paramCount, params);
        String paramsString = params == null ? "" : String.join(",", params);
        return function.toUpperCase() + '(' + paramsString + ')';
    }

    protected void validateParams(@NotNull DataSourceDialect dialect, int paramCount, String... params) {
        if (params == null ? paramCount != 0 : params.length != paramCount)
            throw new PlatformServiceUnavailableException("error.msg.database.function.invalid", "Number of parameters " + Arrays.toString(params) +
                    " must be " + paramCount + " on " + this);
    }
}