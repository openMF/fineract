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
package org.apache.fineract.infrastructure.dataqueries.service;

import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.boot.db.JdbcJavaType;
import org.apache.fineract.infrastructure.core.exception.PlatformDataIntegrityException;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.infrastructure.dataqueries.data.GenericResultsetData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnValueData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetRowData;
import org.apache.fineract.infrastructure.dataqueries.exception.DatatableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData.DisplayType.DATE;
import static org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData.DisplayType.DATETIME;
import static org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData.DisplayType.DECIMAL;
import static org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData.DisplayType.INTEGER;
import static org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData.DisplayType.TIME;

@Service
public class GenericDataServiceImpl implements GenericDataService {

    private static final Logger LOG = LoggerFactory.getLogger(GenericDataServiceImpl.class);

    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;
    private final DataSourceSqlResolver sqlResolver;

    @Autowired
    public GenericDataServiceImpl(final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(this.dataSource);
        this.sqlResolver = sqlResolver;
    }

    @Override
    public GenericResultsetData fillGenericResultSet(final String sql) {
        try {
            final SqlRowSet rs = this.jdbcTemplate.queryForRowSet(sql);

            final List<ResultsetColumnHeaderData> columnHeaders = new ArrayList<>();
            final List<ResultsetRowData> resultsetDataRows = new ArrayList<>();

            final SqlRowSetMetaData rsmd = rs.getMetaData();

            for (int i = 0; i < rsmd.getColumnCount(); i++) {

                final String columnName = rsmd.getColumnName(i + 1);
                final String columnType = rsmd.getColumnTypeName(i + 1);

                final ResultsetColumnHeaderData columnHeader = ResultsetColumnHeaderData.basic(columnName, columnType, sqlResolver.getDialect());
                columnHeaders.add(columnHeader);
            }

            while (rs.next()) {
                final List<String> columnValues = new ArrayList<>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    final String columnName = rsmd.getColumnName(i + 1);
                    final String columnValue = rs.getString(columnName);
                    columnValues.add(columnValue);
                }

                final ResultsetRowData resultsetDataRow = ResultsetRowData.create(columnValues);
                resultsetDataRows.add(resultsetDataRow);
            }

            return new GenericResultsetData(columnHeaders, resultsetDataRows);
        } catch (DataAccessException e) {
            throw new PlatformDataIntegrityException("error.msg.report.unknown.data.integrity.issue", e.getClass().getName(), e);
        }
    }

    @Override
    public String replace(final String str, final String pattern, final String replace) {
        // JPW - this replace may / may not be any better or quicker than the
        // apache stringutils equivalent. It works, but if someone shows the
        // apache one to be about the same then this can be removed.
        int s = 0;
        int e = 0;
        final StringBuilder result = new StringBuilder();

        while ((e = str.indexOf(pattern, s)) >= 0) {
            result.append(str.substring(s, e));
            result.append(replace);
            s = e + pattern.length();
        }
        result.append(str.substring(s));
        return result.toString();
    }

    @Override
    public String wrapSQL(final String sql) {
        // wrap sql to prevent JDBC sql errors, prevent malicious sql and a
        // CachedRowSetImpl bug

        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7046875 - prevent
        // Invalid Column Name bug in sun's CachedRowSetImpl where it doesn't
        // pick up on label names, only column names
        return "select x.* from (" + sql + ") x";
    }

    @Override
    public String generateJsonFromGenericResultsetData(final GenericResultsetData grs) {

        final StringBuilder writer = new StringBuilder();

        writer.append("[");

        final List<ResultsetColumnHeaderData> columnHeaders = grs.getColumnHeaders();

        final List<ResultsetRowData> data = grs.getData();
        List<String> row;
        Integer rSize;
        final String doubleQuote = "\"";
        final String slashDoubleQuote = "\\\"";
        ResultsetColumnHeaderData.DisplayType colDisplayType;
        String currVal;

        for (int i = 0; i < data.size(); i++) {
            writer.append("\n{");

            row = data.get(i).getRow();
            rSize = row.size();
            for (int j = 0; j < rSize; j++) {

                ResultsetColumnHeaderData columnHeader = columnHeaders.get(j);
                writer.append(doubleQuote + columnHeader.getColumnName() + doubleQuote + ": ");
                colDisplayType = columnHeader.getColumnDisplayType();
                final JdbcJavaType colType = columnHeader.getColumnType();
                if (colDisplayType == null) {
                    colDisplayType = ResultsetColumnHeaderData.calcColumnDisplayType(colType);
                }

                currVal = row.get(j);
                if (currVal != null && colDisplayType != null) {
                    if (colDisplayType == DECIMAL || colDisplayType == INTEGER) {
                        writer.append(currVal);
                    } else {
                        if (colDisplayType == DATE) {
                            final LocalDate localDate = LocalDate.parse(currVal);
                            writer.append("[" + localDate.getYear() + ", " + localDate.getMonthValue() + ", " + localDate.getDayOfMonth() + "]");
                        } else if (colDisplayType == DATETIME) {
                            final LocalDateTime localDateTime = LocalDateTime.parse(currVal);
                            writer.append("[" + localDateTime.getYear() + ", " + localDateTime.getMonthValue() + ", "
                                    + localDateTime.getDayOfMonth() + " " + localDateTime.getHour() + ", " + localDateTime.getMinute()
                                    + ", " + localDateTime.getSecond() + ", " + localDateTime.get(ChronoField.MILLI_OF_SECOND) + "]");
                        } else if (colDisplayType == TIME) {
                            final LocalTime localTime = LocalTime.parse(currVal);
                            writer.append("[" + localTime.getHour() + ", "
                                    + localTime.getMinute() + ", " + localTime.getSecond() + ", "
                                    + localTime.get(ChronoField.MILLI_OF_SECOND) + "]");
                        } else {
                            writer.append(doubleQuote + replace(currVal, doubleQuote, slashDoubleQuote) + doubleQuote);
                        }
                    }
                } else {
                    writer.append("null");
                }
                if (j < (rSize - 1)) {
                    writer.append(",\n");
                }
            }

            if (i < (data.size() - 1)) {
                writer.append("},");
            } else {
                writer.append("}");
            }
        }

        writer.append("\n]");
        return writer.toString();

    }

    @Override
    public List<ResultsetColumnHeaderData> fillResultsetColumnHeaders(final String datatable) {

        LOG.debug("::3 Was inside the fill ResultSetColumnHeader");

        final SqlRowSet columnDefinitions = getDatatableMetaData(datatable);

        final List<ResultsetColumnHeaderData> columnHeaders = new ArrayList<>();

        columnDefinitions.beforeFirst();
        while (columnDefinitions.next()) {
            final String columnName = columnDefinitions.getString("COLUMN_NAME");
            final String isNullable = columnDefinitions.getString("IS_NULLABLE");
            final String isPrimaryKey = columnDefinitions.getString("COLUMN_KEY");
            final String columnType = columnDefinitions.getString("DATA_TYPE");
            final Long columnLength = columnDefinitions.getLong("CHARACTER_MAXIMUM_LENGTH");

            final boolean columnNullable = "YES".equalsIgnoreCase(isNullable);
            final boolean columnIsPrimaryKey = "PRI".equalsIgnoreCase(isPrimaryKey);

            JdbcJavaType jdbcType = JdbcJavaType.getByTypeName(sqlResolver.getDialect(), columnType);

            List<ResultsetColumnValueData> columnValues = new ArrayList<>();
            String codeName = null;
            if (jdbcType != null && jdbcType.isVarcharType()) {
                final int codePosition = columnName.indexOf("_cv_");
                if (codePosition > 0) {
                    codeName = columnName.substring(0, codePosition);

                    columnValues = retreiveColumnValues(codeName);
                }
            } else if (jdbcType != null && jdbcType.isIntegerType()) {
                final int codePosition = columnName.indexOf("_cd_");
                if (codePosition > 0) {
                    codeName = columnName.substring(0, codePosition);
                    columnValues = retreiveColumnValues(codeName);
                }
            }
            if (codeName == null) {
                final SqlRowSet rsValues = getDatatableCodeData(datatable, columnName);
                Integer codeId = null;
                while (rsValues.next()) {
                    codeId = rsValues.getInt("id");
                    codeName = rsValues.getString("code_name");
                }
                columnValues = retreiveColumnValues(codeId);

            }

            final ResultsetColumnHeaderData rsch = ResultsetColumnHeaderData.detailed(columnName, columnType, columnLength, columnNullable,
                    columnIsPrimaryKey, columnValues, codeName, sqlResolver.getDialect());

            columnHeaders.add(rsch);
        }

        return columnHeaders;
    }

    /*
     * Candidate for using caching there to get allowed 'column values' from code/codevalue tables
     */
    private List<ResultsetColumnValueData> retreiveColumnValues(final String codeName) {

        final List<ResultsetColumnValueData> columnValues = new ArrayList<>();

        final String sql = "select v.id, v.code_score, v.code_value from m_code m " + " join m_code_value v on v.code_id = m.id "
                + " where m.code_name = '" + codeName + "' order by v.order_position, v.id";

        final SqlRowSet rsValues = this.jdbcTemplate.queryForRowSet(sql);

        rsValues.beforeFirst();
        while (rsValues.next()) {
            final Integer id = rsValues.getInt("id");
            final String codeValue = rsValues.getString("code_value");
            final Integer score = rsValues.getInt("code_score");

            columnValues.add(new ResultsetColumnValueData(id, codeValue, score));
        }

        return columnValues;
    }

    private List<ResultsetColumnValueData> retreiveColumnValues(final Integer codeId) {

        final List<ResultsetColumnValueData> columnValues = new ArrayList<>();
        if (codeId != null) {
            final String sql = "select v.id, v.code_value from m_code_value v where v.code_id =" + codeId
                    + " order by v.order_position, v.id";
            final SqlRowSet rsValues = this.jdbcTemplate.queryForRowSet(sql);
            rsValues.beforeFirst();
            while (rsValues.next()) {
                final Integer id = rsValues.getInt("id");
                final String codeValue = rsValues.getString("code_value");
                columnValues.add(new ResultsetColumnValueData(id, codeValue));
            }
        }

        return columnValues;
    }

    private SqlRowSet getDatatableMetaData(final String datatable) {
        String sql = "SELECT c.COLUMN_NAME, c.IS_NULLABLE, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, ";
        sql += sqlResolver.getDialect().isPostgres()
                ? ("(CASE WHEN EXISTS (SELECT 1 FROM information_schema.table_constraints tc"
                    + " JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)"
                    + " WHERE tc.constraint_schema = c.table_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name AND tc.constraint_type = 'PRIMARY KEY')"
                    + " THEN 'PRI' ELSE NULL END) AS COLUMN_KEY"
                    + " FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = current_schema() AND c.TABLE_NAME = '" + datatable + "' ")
                : ("c.COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS c WHERE TABLE_SCHEMA = schema() AND TABLE_NAME = '" + datatable + "' ");

        sql += "ORDER BY ORDINAL_POSITION";

        final SqlRowSet columnDefinitions = this.jdbcTemplate.queryForRowSet(sql);
        if (columnDefinitions.next()) {
            return columnDefinitions;
        }

        throw new DatatableNotFoundException(datatable);
    }

    private SqlRowSet getDatatableCodeData(final String datatable, final String columnName) {

        final String sql = "select mc.id,mc.code_name from m_code mc join x_table_column_code_mappings xcc on xcc.code_id = mc.id where xcc.column_alias_name='"
                + datatable.toLowerCase().replaceAll("\\s", "_") + "_" + columnName + "'";
        final SqlRowSet rsValues = this.jdbcTemplate.queryForRowSet(sql);

        return rsValues;
    }
}
