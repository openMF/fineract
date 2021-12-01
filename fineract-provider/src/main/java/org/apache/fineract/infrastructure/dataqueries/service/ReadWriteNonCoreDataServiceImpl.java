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

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.persistence.PersistenceException;
import javax.sql.DataSource;
import javax.validation.constraints.NotNull;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.fineract.infrastructure.codes.service.CodeReadPlatformService;
import org.apache.fineract.infrastructure.configuration.domain.ConfigurationDomainService;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.boot.db.DataSourceDialect;
import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.boot.db.JdbcJavaType;
import org.apache.fineract.infrastructure.core.data.ApiParameterError;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResultBuilder;
import org.apache.fineract.infrastructure.core.data.DataValidatorBuilder;
import org.apache.fineract.infrastructure.core.exception.GeneralPlatformDomainRuleException;
import org.apache.fineract.infrastructure.core.exception.PlatformApiDataValidationException;
import org.apache.fineract.infrastructure.core.exception.PlatformDataIntegrityException;
import org.apache.fineract.infrastructure.core.exception.PlatformServiceUnavailableException;
import org.apache.fineract.infrastructure.core.serialization.DatatableCommandFromApiJsonDeserializer;
import org.apache.fineract.infrastructure.core.serialization.FromJsonHelper;
import org.apache.fineract.infrastructure.core.serialization.JsonParserHelper;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.infrastructure.dataqueries.api.DataTableApiConstant;
import org.apache.fineract.infrastructure.dataqueries.data.DataTableValidator;
import org.apache.fineract.infrastructure.dataqueries.data.DatatableData;
import org.apache.fineract.infrastructure.dataqueries.data.GenericResultsetData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetRowData;
import org.apache.fineract.infrastructure.dataqueries.exception.DatatableEntryRequiredException;
import org.apache.fineract.infrastructure.dataqueries.exception.DatatableNotFoundException;
import org.apache.fineract.infrastructure.dataqueries.exception.DatatableSystemErrorException;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.apache.fineract.infrastructure.security.utils.ColumnValidator;
import org.apache.fineract.infrastructure.security.utils.SQLInjectionValidator;
import org.apache.fineract.useradministration.domain.AppUser;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

@Service
public class ReadWriteNonCoreDataServiceImpl implements ReadWriteNonCoreDataService {

    private final static String DATATABLE_NAME_REGEX_PATTERN = "^[a-zA-Z][a-zA-Z0-9\\-_\\s]{0,48}[a-zA-Z0-9]$";

    private final static String CODE_VALUES_TABLE = "m_code_value";

    private final static Logger LOG = LoggerFactory.getLogger(ReadWriteNonCoreDataServiceImpl.class);
    @SuppressWarnings("DoubleBraceInitialization")
    private final static HashMap<String, JdbcJavaType> apiTypeToMySQL = new HashMap<>() {
        {
            put("STRING", JdbcJavaType.VARCHAR);
            put("NUMBER", JdbcJavaType.INTEGER);
            put("BOOLEAN", JdbcJavaType.BOOLEAN);
            put("DECIMAL", JdbcJavaType.DECIMAL);
            put("DATE", JdbcJavaType.DATE);
            put("DATETIME", JdbcJavaType.DATETIME);
            put("TEXT", JdbcJavaType.TEXT);
            put("DROPDOWN", JdbcJavaType.INTEGER);
            put("CHAR", JdbcJavaType.CHAR);
        }
    };

    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;
    private final DataSourceSqlResolver sqlResolver;

    private final PlatformSecurityContext context;
    private final FromJsonHelper fromJsonHelper;
    private final JsonParserHelper helper;
    private final GenericDataService genericDataService;
    private final DatatableCommandFromApiJsonDeserializer fromApiJsonDeserializer;
    private final ConfigurationDomainService configurationDomainService;
    private final CodeReadPlatformService codeReadPlatformService;
    private final DataTableValidator dataTableValidator;
    private final ColumnValidator columnValidator;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    // private final GlobalConfigurationWritePlatformServiceJpaRepositoryImpl
    // configurationWriteService;

    @Autowired(required = true)
    public ReadWriteNonCoreDataServiceImpl(final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver,
                                           final PlatformSecurityContext context,
            final FromJsonHelper fromJsonHelper, final GenericDataService genericDataService,
            final DatatableCommandFromApiJsonDeserializer fromApiJsonDeserializer, final CodeReadPlatformService codeReadPlatformService,
            final ConfigurationDomainService configurationDomainService, final DataTableValidator dataTableValidator,
            final ColumnValidator columnValidator) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(this.dataSource);
        this.sqlResolver = sqlResolver;
        this.context = context;
        this.fromJsonHelper = fromJsonHelper;
        this.helper = new JsonParserHelper();
        this.genericDataService = genericDataService;
        this.fromApiJsonDeserializer = fromApiJsonDeserializer;
        this.codeReadPlatformService = codeReadPlatformService;
        this.configurationDomainService = configurationDomainService;
        this.dataTableValidator = dataTableValidator;
        this.columnValidator = columnValidator;
        // this.configurationWriteService = configurationWriteService;
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    @Override
    public List<DatatableData> retrieveDatatableNames(final String appTable) {

        String andClause;
        if (appTable == null) {
            andClause = "";
        } else {
            validateAppTable(appTable);
            SQLInjectionValidator.validateSQLInput(appTable);
            andClause = " and application_table_name = '" + appTable + "'";
        }

        // PERMITTED datatables
        final String sql = "select application_table_name, registered_table_name" + " from x_registered_table " + " where exists"
                + " (select 'f'" + " from m_appuser_role ur " + " join m_role r on r.id = ur.role_id"
                + " left join m_role_permission rp on rp.role_id = r.id" + " left join m_permission p on p.id = rp.permission_id"
                + " where ur.appuser_id = " + this.context.authenticatedUser().getId()
                + " and (p.code in ('ALL_FUNCTIONS', 'ALL_FUNCTIONS_READ') or p.code = concat('READ_', registered_table_name))) "
                + andClause + " order by application_table_name, registered_table_name";

        final SqlRowSet rs = this.jdbcTemplate.queryForRowSet(sql);

        final List<DatatableData> datatables = new ArrayList<>();
        while (rs.next()) {
            final String appTableName = rs.getString("application_table_name");
            final String registeredDatatableName = rs.getString("registered_table_name");
            final List<ResultsetColumnHeaderData> columnHeaderData = this.genericDataService
                    .fillResultsetColumnHeaders(registeredDatatableName);

            datatables.add(DatatableData.create(appTableName, registeredDatatableName, columnHeaderData));
        }

        return datatables;
    }

    @Override
    public DatatableData retrieveDatatable(final String datatable) {

        // PERMITTED datatables
        SQLInjectionValidator.validateSQLInput(datatable);
        final String sql = "select application_table_name, registered_table_name" + " from x_registered_table " + " where exists"
                + " (select 'f'" + " from m_appuser_role ur " + " join m_role r on r.id = ur.role_id"
                + " left join m_role_permission rp on rp.role_id = r.id" + " left join m_permission p on p.id = rp.permission_id"
                + " where ur.appuser_id = " + this.context.authenticatedUser().getId() + " and registered_table_name='" + datatable + "'"
                + " and (p.code in ('ALL_FUNCTIONS', 'ALL_FUNCTIONS_READ') or p.code = concat('READ_', registered_table_name))) "
                + " order by application_table_name, registered_table_name";

        final SqlRowSet rs = this.jdbcTemplate.queryForRowSet(sql);

        DatatableData datatableData = null;
        while (rs.next()) {
            final String appTableName = rs.getString("application_table_name");
            final String registeredDatatableName = rs.getString("registered_table_name");
            final List<ResultsetColumnHeaderData> columnHeaderData = this.genericDataService
                    .fillResultsetColumnHeaders(registeredDatatableName);

            datatableData = DatatableData.create(appTableName, registeredDatatableName, columnHeaderData);
        }

        return datatableData;
    }

    @Transactional
    @Override
    public void registerDatatable(final String dataTableName, final String applicationTableName) {

        Integer category = DataTableApiConstant.CATEGORY_DEFAULT;

        final String permissionSql = this._getPermissionSql(dataTableName);
        this._registerDataTable(applicationTableName, dataTableName, category, permissionSql);

    }

    @Transactional
    @Override
    public void registerDatatable(final JsonCommand command) {

        final String applicationTableName = this.getTableName(command.getUrl());
        final String dataTableName = this.getDataTableName(command.getUrl());

        Integer category = this.getCategory(command);

        this.dataTableValidator.validateDataTableRegistration(command.json());
        final String permissionSql = this._getPermissionSql(dataTableName);
        this._registerDataTable(applicationTableName, dataTableName, category, permissionSql);

    }

    @Transactional
    @Override
    public void registerDatatable(final JsonCommand command, final String permissionSql) {
        final String applicationTableName = this.getTableName(command.getUrl());
        final String dataTableName = this.getDataTableName(command.getUrl());

        Integer category = this.getCategory(command);

        this.dataTableValidator.validateDataTableRegistration(command.json());

        this._registerDataTable(applicationTableName, dataTableName, category, permissionSql);

    }

    private void _registerDataTable(final String applicationTableName, final String dataTableName, final Integer category,
            final String permissionsSql) {

        validateAppTable(applicationTableName);
        validateDatatableName(dataTableName);
        assertDataTableExists(dataTableName);

        Map<String, Object> paramMap = new HashMap<>(3);
        final String registerDatatableSql = "insert into x_registered_table (registered_table_name, application_table_name,category) values ( :dataTableName, :applicationTableName, :category)";
        paramMap.put("dataTableName", dataTableName);
        paramMap.put("applicationTableName", applicationTableName);
        paramMap.put("category", category);

        try {
            this.namedParameterJdbcTemplate.update(registerDatatableSql, paramMap);
            this.jdbcTemplate.update(permissionsSql);

            // add the registered table to the config if it is a ppi
            if (this.isSurveyCategory(category)) {
                String enabled = sqlResolver.formatBoolValue(false);
                this.namedParameterJdbcTemplate.update("insert into c_configuration (name, value, enabled ) values( :dataTableName , '0', " + enabled + ")", paramMap);
            }

        } catch (final JpaSystemException | DataIntegrityViolationException dve) {
            final Throwable cause = dve.getCause();
            final Throwable realCause = dve.getMostSpecificCause();
            // even if duplicate is only due to permission duplicate, okay to
            // show duplicate datatable error msg
            if (realCause.getMessage().contains("Duplicate entry") || cause.getMessage().contains("Duplicate entry")) {
                throw new PlatformDataIntegrityException("error.msg.datatable.registered",
                        "Datatable `" + dataTableName + "` is already registered against an application table.", "dataTableName",
                        dataTableName, dve);
            }
            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", dve);
        } catch (final PersistenceException dve) {
            final Throwable cause = dve.getCause();
            if (cause.getMessage().contains("Duplicate entry")) {
                throw new PlatformDataIntegrityException("error.msg.datatable.registered",
                        "Datatable `" + dataTableName + "` is already registered against an application table.", "dataTableName",
                        dataTableName, dve);
            }
            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", dve);
        }

    }

    private String _getPermissionSql(final String dataTableName) {
        final String createPermission = "'CREATE_" + dataTableName + "'";
        final String createPermissionChecker = "'CREATE_" + dataTableName + "_CHECKER'";
        final String readPermission = "'READ_" + dataTableName + "'";
        final String updatePermission = "'UPDATE_" + dataTableName + "'";
        final String updatePermissionChecker = "'UPDATE_" + dataTableName + "_CHECKER'";
        final String deletePermission = "'DELETE_" + dataTableName + "'";
        final String deletePermissionChecker = "'DELETE_" + dataTableName + "_CHECKER'";

        return "insert into m_permission (grouping, code, action_name, entity_name, can_maker_checker) values " + "('datatable', "
                + createPermission + ", 'CREATE', '" + dataTableName + "', true)," + "('datatable', " + createPermissionChecker
                + ", 'CREATE', '" + dataTableName + "', false)," + "('datatable', " + readPermission + ", 'READ', '" + dataTableName
                + "', false)," + "('datatable', " + updatePermission + ", 'UPDATE', '" + dataTableName + "', true)," + "('datatable', "
                + updatePermissionChecker + ", 'UPDATE', '" + dataTableName + "', false)," + "('datatable', " + deletePermission
                + ", 'DELETE', '" + dataTableName + "', true)," + "('datatable', " + deletePermissionChecker + ", 'DELETE', '"
                + dataTableName + "', false)";

    }

    private Integer getCategory(final JsonCommand command) {
        Integer category = command.integerValueOfParameterNamedDefaultToNullIfZero(DataTableApiConstant.categoryParamName);
        if (category == null) {
            category = DataTableApiConstant.CATEGORY_DEFAULT;
        }
        return category;
    }

    private boolean isSurveyCategory(final Integer category) {
        return category.equals(DataTableApiConstant.CATEGORY_PPI);
    }

    @Override
    public String getDataTableName(String url) {
        return Splitter.on('/').splitToList(url).get(3);
    }

    @Override
    public String getTableName(String url) {
        return Splitter.on('/').splitToList(url).get(4);
    }

    @Transactional
    @Override
    public void deregisterDatatable(final String datatable) {
        validateDatatableName(datatable);
        final String permissionList = "('CREATE_" + datatable + "', 'CREATE_" + datatable + "_CHECKER', 'READ_" + datatable + "', 'UPDATE_"
                + datatable + "', 'UPDATE_" + datatable + "_CHECKER', 'DELETE_" + datatable + "', 'DELETE_" + datatable + "_CHECKER')";

        final String deleteRolePermissionsSql = "delete from m_role_permission where m_role_permission.permission_id in (select id from m_permission where code in "
                + permissionList + ")";

        final String deletePermissionsSql = "delete from m_permission where code in " + permissionList;

        final String deleteRegisteredDatatableSql = "delete from x_registered_table where registered_table_name = '" + datatable + "'";

        final String deleteFromConfigurationSql = "delete from c_configuration where name ='" + datatable + "'";

        String[] sqlArray = new String[4];
        sqlArray[0] = deleteRolePermissionsSql;
        sqlArray[1] = deletePermissionsSql;
        sqlArray[2] = deleteRegisteredDatatableSql;
        sqlArray[3] = deleteFromConfigurationSql;

        this.jdbcTemplate.batchUpdate(sqlArray);
    }

    @Transactional
    @Override
    public CommandProcessingResult createNewDatatableEntry(final String dataTableName, final Long appTableId, final JsonCommand command) {
        return createNewDatatableEntry(dataTableName, appTableId, command.json());
    }

    @Transactional
    @Override
    public CommandProcessingResult createNewDatatableEntry(final String dataTableName, final Long appTableId, final String json) {
        try {
            final String appTable = queryForApplicationTableName(dataTableName);
            final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);

            final List<ResultsetColumnHeaderData> columnHeaders = this.genericDataService.fillResultsetColumnHeaders(dataTableName);

            final Type typeOfMap = new TypeToken<Map<String, String>>() {}.getType();
            final Map<String, String> dataParams = this.fromJsonHelper.extractDataMap(typeOfMap, json);

            final String sql = getAddSql(columnHeaders, dataTableName, getFKField(appTable), appTableId, dataParams);

            this.jdbcTemplate.update(sql);

            return commandProcessingResult; //

        } catch (final DataAccessException dve) {
            final Throwable cause = dve.getCause();
            final Throwable realCause = dve.getMostSpecificCause();
            if (realCause.getMessage().contains("Duplicate entry") || cause.getMessage().contains("Duplicate entry")) {
                throw new PlatformDataIntegrityException("error.msg.datatable.entry.duplicate", "An entry already exists for datatable `"
                        + dataTableName + "` and application table with identifier `" + appTableId + "`.", "dataTableName", dataTableName,
                        appTableId);
            } else if (realCause.getMessage().contains("doesn't have a default value")
                    || cause.getMessage().contains("doesn't have a default value")) {
                throw new PlatformDataIntegrityException(
                    "error.msg.datatable.no.value.provided.for.required.fields", "No values provided for the datatable `" + dataTableName
                            + "` and application table with identifier `" + appTableId + "`.", "dataTableName", dataTableName, appTableId); }

            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.");
        } catch (final PersistenceException e) {
            final Throwable cause = e.getCause();
            if (cause.getMessage().contains("Duplicate entry")) {
                throw new PlatformDataIntegrityException("error.msg.datatable.entry.duplicate", "An entry already exists for datatable `"
                        + dataTableName + "` and application table with identifier `" + appTableId + "`.", "dataTableName", dataTableName,
                        appTableId);
            } else if (cause.getMessage().contains("doesn't have a default value")) { throw new PlatformDataIntegrityException(
                    "error.msg.datatable.no.value.provided.for.required.fields", "No values provided for the datatable `" + dataTableName
                            + "` and application table with identifier `" + appTableId + "`.", "dataTableName", dataTableName, appTableId); }

            logAsErrorUnexpectedDataIntegrityException(e);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.");

        }
    }

    private String getAddSql(final List<ResultsetColumnHeaderData> columnHeaders, final String datatable, final String fkName,
                             final Long appTableId, final Map<String, String> queryParams) {

        final Map<String, String> affectedColumns = getAffectedColumns(columnHeaders, queryParams, fkName);

        String pValueWrite = "";
        String addSql = "";
        final String singleQuote = "'";

        String insertColumns = "";
        String selectColumns = "";
        String columnName = "";
        String pValue = null;
        for (final ResultsetColumnHeaderData columnHeader : columnHeaders) {
            final String key = columnHeader.getColumnName();
            if (affectedColumns.containsKey(key)) {
                pValue = affectedColumns.get(key);
                if (StringUtils.isEmpty(pValue)) {
                    pValueWrite = "null";
                } else {
                    JdbcJavaType columnType = columnHeader.getColumnType();
                    Boolean boolValue = columnType.canBooleanType(sqlResolver.getDialect()) ? BooleanUtils.toBooleanObject(pValue) : null;
                    if (boolValue != null) {
                        pValueWrite = sqlResolver.formatJdbcValue(columnType, sqlResolver.toJdbcValue(columnType, boolValue));
                    } else {
                        pValueWrite = singleQuote + this.genericDataService.replace(pValue, singleQuote, singleQuote + singleQuote)
                                + singleQuote;
                    }

                }
                columnName = key;
                insertColumns += ", " + sqlResolver.toDefinition(columnName);
                selectColumns += "," + pValueWrite + " as " + sqlResolver.toDefinition(columnName);
            }
        }

        addSql = "insert into " + sqlResolver.toDefinition(datatable) + " (" + sqlResolver.toDefinition(fkName) + " " + insertColumns + ")" + " select " + appTableId + " as id" + selectColumns;

        LOG.debug(addSql);

        return addSql;
    }

    @Override
    public CommandProcessingResult createPPIEntry(final String dataTableName, final Long appTableId, final JsonCommand command) {

        try {
            final String appTable = queryForApplicationTableName(dataTableName);
            final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);

            final List<ResultsetColumnHeaderData> columnHeaders = this.genericDataService.fillResultsetColumnHeaders(dataTableName);

            final Type typeOfMap = new TypeToken<Map<String, String>>() {}.getType();
            final Map<String, String> dataParams = this.fromJsonHelper.extractDataMap(typeOfMap, command.json());

            final String sql = getAddSqlWithScore(columnHeaders, dataTableName, getFKField(appTable), appTableId, dataParams);

            this.jdbcTemplate.update(sql);

            return commandProcessingResult; //

        } catch (final DataAccessException dve) {
            final Throwable cause = dve.getCause();
            final Throwable realCause = dve.getMostSpecificCause();
            if (realCause.getMessage().contains("Duplicate entry") || cause.getMessage().contains("Duplicate entry")) {
                throw new PlatformDataIntegrityException(
                        "error.msg.datatable.entry.duplicate", "An entry already exists for datatable `" + dataTableName
                                + "` and application table with identifier `" + appTableId + "`.",
                            "dataTableName", dataTableName, appTableId); }

            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.");
        } catch (final PersistenceException dve) {
            final Throwable cause = dve.getCause();
            if (cause.getMessage().contains("Duplicate entry")) {
                throw new PlatformDataIntegrityException(
                        "error.msg.datatable.entry.duplicate", "An entry already exists for datatable `" + dataTableName
                                + "` and application table with identifier `" + appTableId + "`.",
                            "dataTableName", dataTableName, appTableId); }

            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.");
            }
        }

    @Transactional
    @Override
    public CommandProcessingResult createDatatable(final JsonCommand command) {

        String datatableName = null;

        try {
            this.context.authenticatedUser();
            this.fromApiJsonDeserializer.validateForCreate(command.json());

            final JsonElement element = this.fromJsonHelper.parse(command.json());
            final JsonArray columns = this.fromJsonHelper.extractJsonArrayNamed("columns", element);
            datatableName = this.fromJsonHelper.extractStringNamed("datatableName", element);
            final String apptableName = this.fromJsonHelper.extractStringNamed("apptableName", element);
            Boolean multiRow = this.fromJsonHelper.extractBooleanNamed("multiRow", element);

            if (multiRow == null) {
                multiRow = false;
            }

            validateDatatableName(datatableName);
            validateAppTable(apptableName);
            final boolean isConstraintApproach = this.configurationDomainService.isConstraintApproachEnabledForDatatables();
            final String fkColumnName = apptableName.substring(2) + "_id";
            final String dataTableNameAlias = datatableName.toLowerCase().replaceAll("\\s", "_");
            StringBuilder sqlBuilder = new StringBuilder();
            sqlResolver.formatCreateTable(sqlBuilder, datatableName).append(" (");

            if (multiRow) {
                sqlResolver.formatCreateTableColumn(sqlBuilder, "id", JdbcJavaType.BIGSERIAL, 20, null, false).append(", ");
            }
            sqlResolver.formatCreateTableColumn(sqlBuilder, fkColumnName, JdbcJavaType.BIGINT, 20, null, false, null).append(", ");

            final Map<String, Long> codeMappings = new HashMap<>();
            final StringBuilder constraintBuilder = new StringBuilder();
            for (final JsonElement column : columns) {
                parseDatatableColumnObjectForCreate(column.getAsJsonObject(), sqlBuilder, constraintBuilder, dataTableNameAlias,
                        codeMappings, isConstraintApproach);
            }

            // Trailing comma and space was added in parseDatatableColumnObjectForCreate
            sqlResolver.formatCreateTablePk(sqlBuilder, multiRow ? "id" : fkColumnName).append(", ");

            /*
             * In cases of tables storing hierarchical entities (like m_group),
             * different entities would end up being stored in the same table.
             *
             * Ex: Centers are a specific type of group, add abstractions for
             * the same
             */
            final String actualAppTableName = mapToActualAppTable(apptableName);
            final String fkName = dataTableNameAlias + "_" + fkColumnName;
            sqlResolver.formatCreateTableFk(sqlBuilder, "fk_" + fkName, fkColumnName, actualAppTableName, "id");

            sqlBuilder.append(constraintBuilder).append(")");
            sqlResolver.formatCreateTableSettings(sqlBuilder, true).append(";");

            if (multiRow) {
                sqlResolver.formatAddTableIndex(sqlBuilder, datatableName, fkName, fkColumnName);
            }
            this.jdbcTemplate.execute(sqlBuilder.toString());

            registerDatatable(datatableName, actualAppTableName);
            registerColumnCodeMapping(codeMappings);
        } catch (final JpaSystemException | DataIntegrityViolationException e) {
            final Throwable realCause = e.getCause();
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");

            if (realCause.getMessage().toLowerCase().contains("duplicate column name")) {
                baseDataValidator.reset().parameter("name").failWithCode("duplicate.column.name");
            } else if (realCause.getMessage().contains("Table") && realCause.getMessage().contains("already exists")) {
                baseDataValidator.reset().parameter("datatableName").value(datatableName).failWithCode("datatable.already.exists");
            } else if (realCause.getMessage().contains("Column") && realCause.getMessage().contains("big")) {
                baseDataValidator.reset().parameter("column").failWithCode("length.too.big");
            } else if (realCause.getMessage().contains("Row") && realCause.getMessage().contains("large")) {
                baseDataValidator.reset().parameter("row").failWithCode("size.too.large");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        } catch (final PersistenceException | BadSqlGrammarException ee) {
            Throwable realCause = ExceptionUtils.getRootCause(ee.getCause());
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");
            if (realCause.getMessage().toLowerCase().contains("duplicate column name")) {
                baseDataValidator.reset().parameter("name").failWithCode("duplicate.column.name");
            } else if (realCause.getMessage().contains("Table") && realCause.getMessage().contains("already exists")) {
                baseDataValidator.reset().parameter("datatableName").value(datatableName).failWithCode("datatable.already.exists");
            } else if (realCause.getMessage().contains("Column") && realCause.getMessage().contains("big")) {
                baseDataValidator.reset().parameter("column").failWithCode("length.too.big");
            } else if (realCause.getMessage().contains("Row") && realCause.getMessage().contains("large")) {
                baseDataValidator.reset().parameter("row").failWithCode("size.too.large");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        }

        return new CommandProcessingResultBuilder().withCommandId(command.commandId()).withResourceIdAsString(datatableName).build();
    }

    private void parseDatatableColumnObjectForCreate(final JsonObject column, StringBuilder sqlBuilder,
                                                     final StringBuilder constraintBuilder, final String dataTableNameAlias, final Map<String, Long> codeMappings,
                                                     final boolean isConstraintApproach) {

        String name = column.has("name") ? column.get("name").getAsString() : null;
        final String type = column.has("type") ? column.get("type").getAsString().toUpperCase() : null;
        Integer length = column.has("length") ? column.get("length").getAsInt() : null;
        final boolean mandatory = column.has("mandatory") && column.get("mandatory").getAsBoolean();
        final String code = column.has("code") ? column.get("code").getAsString() : null;

        if (Strings.isNotBlank(code)) {
            if (isConstraintApproach) {
                codeMappings.put(dataTableNameAlias + "_" + name, this.codeReadPlatformService.retriveCode(code).getCodeId());
                constraintBuilder.append(", ");
                sqlResolver.formatCreateTableFk(constraintBuilder, "fk_" + dataTableNameAlias + '_' + name, name, CODE_VALUES_TABLE, "id");
            } else {
                name = datatableColumnNameToCodeValueName(name, code);
            }
        }

        final JdbcJavaType sqlType = apiTypeToMySQL.get(type);
        if (sqlType == null)
            throw new PlatformServiceUnavailableException("error.msg.datatable.column.update.not.allowed", "Column " + name + " type " + type + " is not allowed");
        DataSourceDialect dialect = sqlResolver.getDialect();
        length = adjustColumnLength(dialect, sqlType, length, type);
        Integer scale = adjustColumnScale(dialect, sqlType, null, type);
        sqlResolver.formatCreateTableColumn(sqlBuilder, name, sqlType, length, scale, !mandatory, null).append(", ");
    }

    @Transactional
    @Override
    public void updateDatatable(final String datatableName, final JsonCommand command) {
        try {
            this.context.authenticatedUser();
            this.fromApiJsonDeserializer.validateForUpdate(command.json());

            final JsonElement element = this.fromJsonHelper.parse(command.json());
            final JsonArray changeColumns = this.fromJsonHelper.extractJsonArrayNamed("changeColumns", element);
            final JsonArray addColumns = this.fromJsonHelper.extractJsonArrayNamed("addColumns", element);
            final JsonArray dropColumns = this.fromJsonHelper.extractJsonArrayNamed("dropColumns", element);
            final String apptableName = this.fromJsonHelper.extractStringNamed("apptableName", element);

            validateDatatableName(datatableName);
            int rowCount = getRowCount(datatableName);
            final List<ResultsetColumnHeaderData> columnHeaderData = this.genericDataService.fillResultsetColumnHeaders(datatableName);
            final Map<String, ResultsetColumnHeaderData> mapColumnNameDefinition = new HashMap<>();
            for (final ResultsetColumnHeaderData columnHeader : columnHeaderData) {
                mapColumnNameDefinition.put(columnHeader.getColumnName(), columnHeader);
            }

            final boolean isConstraintApproach = this.configurationDomainService.isConstraintApproachEnabledForDatatables();

            if (!StringUtils.isBlank(apptableName)) {
                validateAppTable(apptableName);

                final String oldApptableName = queryForApplicationTableName(datatableName);
                if (!StringUtils.equals(oldApptableName, apptableName)) {
                    final String oldFKName = oldApptableName.substring(2) + "_id";
                    final String newFKName = apptableName.substring(2) + "_id";
                    final String actualAppTableName = mapToActualAppTable(apptableName);
                    final String oldConstraintName = datatableName.toLowerCase().replaceAll("\\s", "_") + "_" + oldFKName;
                    final String newConstraintName = datatableName.toLowerCase().replaceAll("\\s", "_") + "_" + newFKName;
                    StringBuilder sqlBuilder = new StringBuilder();

                    sqlResolver.formatAlterTable(sqlBuilder, datatableName).append(' ');

                    if (mapColumnNameDefinition.containsKey("id")) {
                        sqlResolver.formatDropTableIndex(sqlBuilder, true, datatableName, "fk_" + oldFKName).append(", ");
                    }
                    sqlResolver.formatDropTableFk(sqlBuilder, true, datatableName, "fk_" + oldConstraintName).append(", ");

                    sqlResolver.formatAlterTableColumn(sqlBuilder, true, datatableName, newFKName, oldFKName, JdbcJavaType.BIGINT, 20, null, false, null, null)
                            .append(", ");
                    sqlResolver.formatAddTableFk(sqlBuilder, true, datatableName, "fk_" + newConstraintName, newFKName, actualAppTableName, "id")
                            .append("; ");
                    sqlResolver.formatAddTableIndex(sqlBuilder, datatableName, "fk_" + newFKName, newFKName);

                    this.jdbcTemplate.execute(sqlBuilder.toString());

                    deregisterDatatable(datatableName);
                    registerDatatable(datatableName, apptableName);
                }
            }

            if (changeColumns == null && addColumns == null && dropColumns == null) {
                return;
            }

            final String datatableAlias = datatableName.toLowerCase().replaceAll("\\s", "_");
            if (dropColumns != null) {
                if(rowCount>0){
                    throw new GeneralPlatformDomainRuleException("error.msg.non.empty.datatable.column.cannot.be.deleted",
                            "Non-empty datatable columns can not be deleted.");
                }

                StringBuilder sqlBuilder = new StringBuilder();
                final List<String> codeMappings = new ArrayList<>();

                sqlResolver.formatAlterTable(sqlBuilder, datatableName).append(' ');

                boolean changed = false;
                for (final JsonElement column : dropColumns) {
                    if (changed)
                        sqlBuilder.append(", ");
                    changed |= buildDropColumnSql(datatableName, datatableAlias, column, sqlBuilder, codeMappings);
                }

                this.jdbcTemplate.execute(sqlBuilder.toString());
                deleteColumnCodeMapping(codeMappings);
            }
            if (addColumns != null) {
                StringBuilder sqlBuilder = new StringBuilder();
                final Map<String, Long> codeMappings = new HashMap<>();

                sqlResolver.formatAlterTable(sqlBuilder, datatableName).append(' ');

                boolean changed = false;
                for (final JsonElement column : addColumns) {
                    if (changed)
                        sqlBuilder.append(", ");
                    changed |= buildAddColumnSql(datatableName, datatableAlias, column, rowCount > 0, isConstraintApproach, sqlBuilder, codeMappings);
                }

                this.jdbcTemplate.execute(sqlBuilder.toString());
                registerColumnCodeMapping(codeMappings);
            }
            if (changeColumns != null) {
                StringBuilder sqlBuilder = new StringBuilder();
                final Map<String, Long> codeMappings = new HashMap<>();
                final List<String> removeMappings = new ArrayList<>();

                sqlResolver.formatAlterTable(sqlBuilder, datatableName).append(' ');

                boolean changed = false;
                for (final JsonElement column : changeColumns) {
                    if (changed)
                        sqlBuilder.append(", ");
                    changed |= buildChangeColumnSql(datatableName, datatableAlias, column, isConstraintApproach, mapColumnNameDefinition, sqlBuilder, codeMappings, removeMappings);
                }
                try {
                    this.jdbcTemplate.execute(sqlBuilder.toString());
                    deleteColumnCodeMapping(removeMappings);
                    registerColumnCodeMapping(codeMappings);
                } catch (final Exception e) {
                    if (e.getMessage().contains("Error on rename")) { throw new PlatformServiceUnavailableException(
                            "error.msg.datatable.column.update.not.allowed", "One of the column name modification not allowed"); }
                    // handle all other exceptions in here
                    // check if exception message contains the "invalid use of null value" SQL exception message throw a 503 HTTP error -
                    // PlatformServiceUnavailableException
                    if (e.getMessage().toLowerCase()
                            .contains("invalid use of null value")) { throw new PlatformServiceUnavailableException(
                            "error.msg.datatable.column.update.not.allowed",
                            "One of the data table columns contains null values"); }
                }
            }
        } catch (final JpaSystemException | DataIntegrityViolationException e) {
            final Throwable realCause = e.getCause();
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");

            if (realCause.getMessage().toLowerCase().contains("unknown column")) {
                baseDataValidator.reset().parameter("name").failWithCode("does.not.exist");
            } else if (realCause.getMessage().toLowerCase().contains("can't drop")) {
                baseDataValidator.reset().parameter("name").failWithCode("does.not.exist");
            } else if (realCause.getMessage().toLowerCase().contains("duplicate column")) {
                baseDataValidator.reset().parameter("name").failWithCode("column.already.exists");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        } catch (final PersistenceException ee) {
            Throwable realCause = ExceptionUtils.getRootCause(ee.getCause());
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");
            if (realCause.getMessage().toLowerCase().contains("duplicate column name")) {
                baseDataValidator.reset().parameter("name").failWithCode("duplicate.column.name");
            } else if (realCause.getMessage().contains("Table") && realCause.getMessage().contains("already exists")) {
                baseDataValidator.reset().parameter("datatableName").value(datatableName).failWithCode("datatable.already.exists");
            } else if (realCause.getMessage().contains("Column") && realCause.getMessage().contains("big")) {
                baseDataValidator.reset().parameter("column").failWithCode("length.too.big");
            } else if (realCause.getMessage().contains("Row") && realCause.getMessage().contains("large")) {
                baseDataValidator.reset().parameter("row").failWithCode("size.too.large");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        }
    }

    private boolean buildAddColumnSql(String tableName, String tableAlias, JsonElement column, boolean hasRows, boolean isConstraintApproach, StringBuilder sqlBuilder, Map<String, Long> codeMappings) {
        JsonObject columnAsJson = column.getAsJsonObject();
        final boolean mandatory = columnAsJson.has("mandatory") && columnAsJson.get("mandatory").getAsBoolean();

        if (hasRows && mandatory) {
            throw new GeneralPlatformDomainRuleException("error.msg.non.empty.datatable.mandatory.column.cannot.be.added",
                    "Non empty datatable mandatory columns can not be added.");
        }

        String name = columnAsJson.has("name") ? columnAsJson.get("name").getAsString() : null;
        if (name == null)
            return false;

        final String type = columnAsJson.has("type") ? columnAsJson.get("type").getAsString().toUpperCase() : null;
        Integer length = columnAsJson.has("length") ? columnAsJson.get("length").getAsInt() : null;
        final String after = columnAsJson.has("after") ? columnAsJson.get("after").getAsString() : null;
        final String code = columnAsJson.has("code") ? columnAsJson.get("code").getAsString() : null;
        String constraintName = tableAlias + "_" + name;

        if (Strings.isNotBlank(code)) {
            if (isConstraintApproach) {
                codeMappings.put(constraintName, this.codeReadPlatformService.retriveCode(code).getCodeId());
                sqlResolver.formatAddTableFk(sqlBuilder, true, tableName, "fk_" + constraintName, name, CODE_VALUES_TABLE, "id").append(", ");
            } else {
                name = datatableColumnNameToCodeValueName(name, code);
            }
        }

        final JdbcJavaType sqlType = apiTypeToMySQL.get(type);
        if (sqlType == null)
            throw new PlatformServiceUnavailableException("error.msg.datatable.column.update.not.allowed", "Column " + name + " type " + type + " is not allowed");

        DataSourceDialect dialect = sqlResolver.getDialect();
        length = adjustColumnLength(dialect, sqlType, length, type);
        Integer scale = adjustColumnScale(dialect, sqlType, null, type);

        sqlResolver.formatAddTableColumn(sqlBuilder, true, tableName, name, sqlType, length, scale, !mandatory, sqlResolver.columnPositionAfter(after));
        return true;
    }

    private boolean buildChangeColumnSql(String tableName, String tableAlias, JsonElement column, boolean isConstraintApproach, Map<String,
            ResultsetColumnHeaderData> mapColumnNameDefinition, StringBuilder sqlBuilder, Map<String, Long> codeMappings, List<String> removeMappings) {
        JsonObject columnAsJson = column.getAsJsonObject();
        String name = columnAsJson.has("name") ? columnAsJson.get("name").getAsString() : null;
        if (name == null)
            return false;

        final boolean mandatory = columnAsJson.has("mandatory") && columnAsJson.get("mandatory").getAsBoolean();
        Integer length = columnAsJson.has("length") ? columnAsJson.get("length").getAsInt() : null;
        String newName = columnAsJson.has("newName") ? columnAsJson.get("newName").getAsString() : name;
        final String after = columnAsJson.has("after") ? columnAsJson.get("after").getAsString() : null;
        final String code = columnAsJson.has("code") ? columnAsJson.get("code").getAsString() : null;
        final String newCode = columnAsJson.has("newCode") ? columnAsJson.get("newCode").getAsString() : null;

        if (isConstraintApproach) {
            if (Strings.isBlank(newName)) {
                newName = name;
            }

            String constraintName = tableAlias + "_" + name;
            String newConstraintName = tableAlias + "_" + newName;

            if (!StringUtils.equalsIgnoreCase(code, newCode) || !StringUtils.equalsIgnoreCase(name, newName)) {
                if (StringUtils.equalsIgnoreCase(code, newCode)) {
                    final int codeId = getCodeIdForColumn(tableAlias, name);
                    if (codeId > 0) {
                        removeMappings.add(constraintName);
                        sqlResolver.formatDropTableFk(sqlBuilder, true, tableName, "fk_" + constraintName).append(", ");
                        codeMappings.put(newConstraintName, (long) codeId);
                        sqlResolver.formatAddTableFk(sqlBuilder, true, tableName, "fk_" + newConstraintName, newName, CODE_VALUES_TABLE, "id")
                                .append(", ");
                    }
                } else {
                    if (code != null) {
                        removeMappings.add(constraintName);
                        if (newCode == null || !StringUtils.equalsIgnoreCase(name, newName))
                            sqlResolver.formatDropTableFk(sqlBuilder, true, tableName, "fk_" + constraintName).append(", ");
                    }
                    if (newCode != null) {
                        codeMappings.put(newConstraintName, this.codeReadPlatformService.retriveCode(newCode).getCodeId());
                        if (code == null || !StringUtils.equalsIgnoreCase(name, newName)) {
                            sqlResolver.formatAddTableFk(sqlBuilder, true, tableName, "fk_" + newConstraintName, newName, CODE_VALUES_TABLE, "id")
                                    .append(", ");
                        }
                    }
                }
            }
        } else {
            if (Strings.isNotBlank(code)) {
                name = datatableColumnNameToCodeValueName(name, code);
                newName = datatableColumnNameToCodeValueName(newName, (Strings.isNotBlank(newCode) ? newCode : code));
            }
        }
        if (!mapColumnNameDefinition.containsKey(name)) {
            throw new PlatformDataIntegrityException("error.msg.datatable.column.missing.update.parse", "Column " + name + " does not exist.", name);
        }

        ResultsetColumnHeaderData columnHeaderData = mapColumnNameDefinition.get(name);
        JdbcJavaType sqlType = columnHeaderData.getColumnType();
        if (sqlType == null)
            throw new PlatformServiceUnavailableException("error.msg.datatable.column.update.not.allowed", "Column " + name + " type is not allowed");

        if (length == null)
            length = columnHeaderData.getColumnLength().intValue();


        // remove NULL values from column where mandatory is true
        removeNullValuesFromStringColumn(tableName, name, sqlType, mandatory);

        DataSourceDialect dialect = sqlResolver.getDialect();
        length = adjustColumnLength(dialect, sqlType, length, null);
        Integer scale = adjustColumnScale(dialect, sqlType, null, null);

        sqlResolver.formatAlterTableColumn(sqlBuilder, true, tableName, newName, name, sqlType, length, scale, !mandatory, null, sqlResolver.columnPositionAfter(after));
        return true;
    }

    private boolean buildDropColumnSql(String tableName, String tableAlias, JsonElement column, StringBuilder sqlBuilder, List<String> codeMappings) {
        JsonObject columnAsJson = column.getAsJsonObject();
        final String name = columnAsJson.has("name") ? columnAsJson.get("name").getAsString() : null;
        if (name == null)
            return false;

        sqlResolver.formatDropTableColumn(sqlBuilder, true, tableName, name).append(", ");

        String constraintName = tableAlias + "_" + name;
        sqlResolver.formatDropTableFk(sqlBuilder, true, tableName, "fk_" + constraintName).append(" ");
        codeMappings.add(constraintName);
        return true;
    }

    /**
     * Update data table, set column value to empty string where current value
     * is NULL. Run update SQL only if the "mandatory" property is set to true
     *
     * @param datatableName
     *            Name of data table
     * @param columnName
     *            name of the column
     * @see {https://mifosforge.jira.com/browse/MIFOSX-1145}
     **/
    private void removeNullValuesFromStringColumn(final String datatableName, String columnName, JdbcJavaType columnType, boolean mandatory) {
        if (!mandatory) {
            return;
        }
        if (!columnType.isStringType() && !columnType.isBlobType()) {
            return;
        }

        String sql = "UPDATE " + datatableName + " SET " + columnName + " = '' WHERE " + columnName + " IS NULL";
        jdbcTemplate.update(sql);
    }

    @Transactional
    @Override
    public void deleteDatatable(final String datatableName) {

        try {
            this.context.authenticatedUser();
            if (!isRegisteredDataTable(datatableName)) {
                throw new DatatableNotFoundException(datatableName);
            }
            validateDatatableName(datatableName);
            assertDataTableEmpty(datatableName);
            deregisterDatatable(datatableName);
            String[] sqlArray = null;
            if (this.configurationDomainService.isConstraintApproachEnabledForDatatables()) {
                final String deleteColumnCodeSql = "delete from x_table_column_code_mappings where column_alias_name like '"
                        + datatableName.toLowerCase().replaceAll("\\s", "_") + "_%'";
                sqlArray = new String[2];
                sqlArray[1] = deleteColumnCodeSql;
            } else {
                sqlArray = new String[1];
            }
            final String sql = "DROP TABLE " + sqlResolver.toDefinition(datatableName);
            sqlArray[0] = sql;
            this.jdbcTemplate.batchUpdate(sqlArray);
        } catch (final JpaSystemException | DataIntegrityViolationException e) {
            final Throwable realCause = e.getCause();
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");
            if (realCause.getMessage().contains("Unknown table")) {
                baseDataValidator.reset().parameter("datatableName").failWithCode("does.not.exist");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        }
    }

    @Transactional
    @Override
    public CommandProcessingResult updateDatatableEntryOneToOne(final String dataTableName, final Long appTableId,
            final JsonCommand command) {

        return updateDatatableEntry(dataTableName, appTableId, null, command);
    }

    @Transactional
    @Override
    public CommandProcessingResult updateDatatableEntryOneToMany(final String dataTableName, final Long appTableId, final Long datatableId,
            final JsonCommand command) {

        return updateDatatableEntry(dataTableName, appTableId, datatableId, command);
    }

    private CommandProcessingResult updateDatatableEntry(final String dataTableName, final Long appTableId, final Long datatableId,
            final JsonCommand command) {

        final String appTable = queryForApplicationTableName(dataTableName);
        final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);

        final GenericResultsetData grs = retrieveDataTableGenericResultSetForUpdate(appTable, dataTableName, appTableId, datatableId);

        if (grs.hasNoEntries()) {
            throw new DatatableNotFoundException(dataTableName, appTableId);
        }

        if (grs.hasMoreThanOneEntry()) {
            throw new PlatformDataIntegrityException("error.msg.attempting.multiple.update",
                    "Application table: " + dataTableName + " Foreign key id: " + appTableId);
        }

        final Type typeOfMap = new TypeToken<Map<String, String>>() {}.getType();
        final Map<String, String> dataParams = this.fromJsonHelper.extractDataMap(typeOfMap, command.json());

        String pkName = "id"; // 1:M datatable
        if (datatableId == null) {
            pkName = getFKField(appTable);
        } // 1:1 datatable

        final Map<String, Object> changes = getAffectedAndChangedColumns(grs, dataParams, pkName);

        if (!changes.isEmpty()) {
            Long pkValue = appTableId;
            if (datatableId != null) {
                pkValue = datatableId;
            }
            final String sql = getUpdateSql(grs.getColumnHeaders(), dataTableName, pkName, pkValue, changes);
            LOG.debug("Update sql: " + sql);
            if (Strings.isNotBlank(sql)) {
                this.jdbcTemplate.update(sql);
                changes.put("locale", dataParams.get("locale"));
                changes.put("dateFormat", "yyyy-MM-dd");
            } else {
                LOG.debug("No Changes");
            }
        }

        return new CommandProcessingResultBuilder() //
                .withOfficeId(commandProcessingResult.getOfficeId()) //
                .withGroupId(commandProcessingResult.getGroupId()) //
                .withClientId(commandProcessingResult.getClientId()) //
                .withSavingsId(commandProcessingResult.getSavingsId()) //
                .withLoanId(commandProcessingResult.getLoanId()) //
                .with(changes) //
                .build();
    }

    private GenericResultsetData retrieveDataTableGenericResultSetForUpdate(final String appTable, final String dataTableName,
                                                                            final Long appTableId, final Long id) {

        final List<ResultsetColumnHeaderData> columnHeaders = this.genericDataService.fillResultsetColumnHeaders(dataTableName);

        String sql = "";

        // id only used for reading a specific entry in a one to many datatable
        // (when updating)
        if (id == null) {
            String whereClause = getFKField(appTable) + " = " + appTableId;
            SQLInjectionValidator.validateSQLInput(whereClause);
            sql = sql + "select * from " + dataTableName + " where " + whereClause;
        } else {
            sql = sql + "select * from " + dataTableName + " where id = " + id;
        }

        final List<ResultsetRowData> result = fillDatatableResultSetDataRows(sql);

        return new GenericResultsetData(columnHeaders, result);
    }

    @Transactional
    @Override
    public CommandProcessingResult deleteDatatableEntries(final String dataTableName, final Long appTableId) {

        validateDatatableName(dataTableName);
        if (isDatatableAttachedToEntityDatatableCheck(dataTableName)) {
            throw new DatatableEntryRequiredException(dataTableName, appTableId);
        }
        final String appTable = queryForApplicationTableName(dataTableName);
        final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);
        final String deleteOneToOneEntrySql = getDeleteEntriesSql(dataTableName, getFKField(appTable), appTableId);

        final int rowsDeleted = this.jdbcTemplate.update(deleteOneToOneEntrySql);
        if (rowsDeleted < 1) {
            throw new DatatableNotFoundException(dataTableName, appTableId);
        }

        return commandProcessingResult;
    }

    @Transactional
    @Override
    public CommandProcessingResult deleteDatatableEntry(final String dataTableName, final Long appTableId, final Long datatableId) {
        validateDatatableName(dataTableName);
        if (isDatatableAttachedToEntityDatatableCheck(dataTableName)) {
            throw new DatatableEntryRequiredException(dataTableName, appTableId);
        }
        final String appTable = queryForApplicationTableName(dataTableName);
        final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);

        final String sql = getDeleteEntrySql(dataTableName, datatableId);

        this.jdbcTemplate.update(sql);
        return commandProcessingResult;
    }

    @Override
    public GenericResultsetData retrieveDataTableGenericResultSet(final String dataTableName, final Long appTableId, final String order,
            final Long id) {

        final String appTable = queryForApplicationTableName(dataTableName);

        checkMainResourceExistsWithinScope(appTable, appTableId);

        final List<ResultsetColumnHeaderData> columnHeaders = this.genericDataService.fillResultsetColumnHeaders(dataTableName);

        String sql = "";

        // id only used for reading a specific entry in a one to many datatable
        // (when updating)
        if (id == null) {
            String whereClause = getFKField(appTable) + " = " + appTableId;
            SQLInjectionValidator.validateSQLInput(whereClause);
            sql = sql + "select * from " + dataTableName + " where " + whereClause;
        } else {
            sql = sql + "select * from " + dataTableName + " where id = " + id;
        }

        if (Strings.isNotBlank(order)) {
            this.columnValidator.validateSqlInjection(sql, order);
            sql = sql + " order by " + order;
        }

        final List<ResultsetRowData> result = fillDatatableResultSetDataRows(sql);

        return new GenericResultsetData(columnHeaders, result);
    }

    @Override
    public Long countDatatableEntries(final String datatableName, final Long appTableId, String foreignKeyColumn) {

        final String sqlString = "SELECT COUNT(" + sqlResolver.toDefinition(foreignKeyColumn) + ") FROM " + sqlResolver.toDefinition(datatableName) +
                " WHERE " + sqlResolver.toDefinition(foreignKeyColumn) + "="  + appTableId;
        return this.jdbcTemplate.queryForObject(sqlString, Long.class);
    }

    // ---- UTIL ----

    private Integer adjustColumnLength(@NotNull DataSourceDialect dialect, JdbcJavaType columnType, Integer length, String inputType) {
        if (!columnType.hasPrecision(dialect))
            return null;
        if (length != null)
            return length;

        if (columnType == JdbcJavaType.DECIMAL)
            length = 19;
        if (inputType != null && inputType.equalsIgnoreCase("Dropdown"))
            length = 11;

        return length;
    }

    private Integer adjustColumnScale(@NotNull DataSourceDialect dialect, JdbcJavaType columnType, Integer scale, String inputType) {
        if (!columnType.hasScale(dialect))
            return null;
        if (scale != null)
            return scale;

        if (columnType == JdbcJavaType.DECIMAL)
            scale = 6;

        return scale;
    }

    private CommandProcessingResult checkMainResourceExistsWithinScope(final String appTable, final Long appTableId) {

        final String sql = dataScopedSQL(appTable, appTableId);
        LOG.debug("data scoped sql: " + sql);
        final SqlRowSet rs = this.jdbcTemplate.queryForRowSet(sql);

        if (!rs.next()) {
            throw new DatatableNotFoundException(appTable, appTableId);
        }

        final Long officeId = getLongSqlRowSet(rs, "officeId");
        final Long groupId = getLongSqlRowSet(rs, "groupId");
        final Long clientId = getLongSqlRowSet(rs, "clientId");
        final Long savingsId = getLongSqlRowSet(rs, "savingsId");
        final Long loanId = getLongSqlRowSet(rs, "loanId");
        final Long productId = getLongSqlRowSet(rs, "productId");
        final Long entityId = getLongSqlRowSet(rs, "entityId");

        if (rs.next()) {
            throw new DatatableSystemErrorException("System Error: More than one row returned from data scoping query");
        }

        return new CommandProcessingResultBuilder() //
                .withOfficeId(officeId) //
                .withGroupId(groupId) //
                .withClientId(clientId) //
                .withSavingsId(savingsId) //
                .withLoanId(loanId)
                .withProductId(productId)
                .withEntityId(entityId)//
                .build();
    }

    private String dataScopedSQL(final String appTable, final Long appTableId) {
        /*
         * unfortunately have to, one way or another, be able to restrict data to the users office hierarchy. Here, a
         * few key tables are done. But if additional fields are needed on other tables the same pattern applies
         */

        final AppUser currentUser = this.context.authenticatedUser();
        String scopedSQL = null;
        /*
         * m_loan and m_savings_account are connected to an m_office thru either an m_client or an m_group If both it
         * means it relates to an m_client that is in a group (still an m_client account)
         */
        if (appTable.equalsIgnoreCase("m_loan")) {
            scopedSQL = "select distinct x.* from ("
                    + " (select o.id as officeId, l.group_id as groupId, l.client_id as clientId, null as savingsId, l.id as loanId, null as productId, null as entityId from m_loan l "
                    + " join m_client c on c.id = l.client_id " + " join m_office o on o.id = c.office_id and o.hierarchy like '"
                    + currentUser.getOffice().getHierarchy() + "%'" + " where l.id = " + appTableId + ")" + " union "
                    + " (select o.id as officeId, l.group_id as groupId, l.client_id as clientId, null as savingsId, l.id as loanId, null as productId, null as entityId from m_loan l "
                    + " join m_group g on g.id = l.group_id " + " join m_office o on o.id = g.office_id and o.hierarchy like '"
                    + currentUser.getOffice().getHierarchy() + "%'" + " where l.id = " + appTableId + ")" + " ) x";
        }
        if (appTable.equalsIgnoreCase("m_savings_account")) {
            scopedSQL = "select distinct x.* from ("
                    + " (select o.id as officeId, s.group_id as groupId, s.client_id as clientId, s.id as savingsId, null as loanId, null as productId, null as entityId from m_savings_account s "
                    + " join m_client c on c.id = s.client_id " + " join m_office o on o.id = c.office_id and o.hierarchy like '"
                    + currentUser.getOffice().getHierarchy() + "%'" + " where s.id = " + appTableId + ")" + " union "
                    + " (select o.id as officeId, s.group_id as groupId, s.client_id as clientId, s.id as savingsId, null as loanId, null as productId, null as entityId from m_savings_account s "
                    + " join m_group g on g.id = s.group_id " + " join m_office o on o.id = g.office_id and o.hierarchy like '"
                    + currentUser.getOffice().getHierarchy() + "%'" + " where s.id = " + appTableId + ")" + " ) x";
        }
        if (appTable.equalsIgnoreCase("m_client")) {
            scopedSQL = "select o.id as officeId, null as groupId, c.id as clientId, null as savingsId, null as loanId, null as productId, null as entityId from m_client c "
                    + " join m_office o on o.id = c.office_id and o.hierarchy like '" + currentUser.getOffice().getHierarchy() + "%'"
                    + " where c.id = " + appTableId;
        }
        if (appTable.equalsIgnoreCase("m_group") || appTable.equalsIgnoreCase("m_center")) {
            scopedSQL = "select o.id as officeId, g.id as groupId, null as clientId, null as savingsId, null as loanId, null as productId, null as entityId from m_group g "
                    + " join m_office o on o.id = g.office_id and o.hierarchy like '" + currentUser.getOffice().getHierarchy() + "%'"
                    + " where g.id = " + appTableId;
        }
        if (appTable.equalsIgnoreCase("m_office")) {
            scopedSQL = "select o.id as officeId, null as groupId, null as clientId, null as savingsId, null as loanId, null as productId, null as entityId from m_office o "
                    + " where o.hierarchy like '" + currentUser.getOffice().getHierarchy() + "%'" + " and o.id = " + appTableId;
        }

        if (appTable.equalsIgnoreCase("m_product_loan")) {
            scopedSQL = "select null as officeId, null as groupId, null as clientId, null as savingsId, null as loanId, p.id as productId, null as entityId from "
                    + sqlResolver.toDefinition(appTable) + " as p WHERE p.id = " + appTableId;
        }
        if (appTable.equalsIgnoreCase("m_savings_product")) {
            scopedSQL = "select null as officeId, null as groupId, null as clientId, null as savingsId, null as loanId, null as productId, p.id as entityId from "
                    + sqlResolver.toDefinition(appTable) + " as p WHERE p.id = " + appTableId;
        }

        if (scopedSQL == null) { throw new PlatformDataIntegrityException("error.msg.invalid.dataScopeCriteria",
                "Application Table: " + appTable + " not catered for in data Scoping"); }

        return scopedSQL;

    }

    private Long getLongSqlRowSet(final SqlRowSet rs, final String column) {
        Long val = rs.getLong(column);
        if (val == 0) {
            val = null;
        }
        return val;
    }

    private boolean isRegisteredDataTable(final String name) {
        // PERMITTED datatables
        final String sql = "select (CASE WHEN exists (select 1 from x_registered_table where registered_table_name = ?) THEN 'true' ELSE 'false' END)";
        final String isRegisteredDataTable = this.jdbcTemplate.queryForObject(sql, String.class, new Object[] { name });
        return Boolean.parseBoolean(isRegisteredDataTable);
    }

    private void assertDataTableExists(final String datatableName) {
        String sql = "select (CASE WHEN exists (select 1 from information_schema.tables where table_schema = "
                + sqlResolver.formatSchema()
                + " and table_name = ?) THEN 'true' ELSE 'false' END)";
        final String dataTableExistsString = this.jdbcTemplate.queryForObject(sql, String.class, new Object[] { datatableName });
        final boolean dataTableExists = Boolean.parseBoolean(dataTableExistsString);
        if (!dataTableExists) { throw new PlatformDataIntegrityException("error.msg.invalid.datatable",
                "Invalid Data Table: " + datatableName, "name", datatableName); }
    }

    private void validateDatatableName(final String name) {

        if (name == null || name.isEmpty()) {
            throw new PlatformDataIntegrityException("error.msg.datatables.datatable.null.name", "Data table name must not be blank.");
        } else if (!name.matches(DATATABLE_NAME_REGEX_PATTERN)) { throw new PlatformDataIntegrityException(
                "error.msg.datatables.datatable.invalid.name.regex", "Invalid data table name.", name); }
        SQLInjectionValidator.validateSQLInput(name);
        }

    private String datatableColumnNameToCodeValueName(final String columnName, final String code) {
        return (code + "_cd_" + columnName);
    }

    private void logAsErrorUnexpectedDataIntegrityException(final Exception dve) {
        LOG.error(dve.getMessage(), dve);
    }

    private void throwExceptionIfValidationWarningsExist(final List<ApiParameterError> dataValidationErrors) {
        if (!dataValidationErrors.isEmpty()) { throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist",
                "Validation errors exist.", dataValidationErrors); }
    }

    @SuppressWarnings("deprecation")
    private int getCodeIdForColumn(final String dataTableNameAlias, final String name) {
        final StringBuilder checkColumnCodeMapping = new StringBuilder();
        checkColumnCodeMapping.append("SELECT ccm.code_id FROM x_table_column_code_mappings ccm WHERE ccm.column_alias_name='")
                .append(dataTableNameAlias).append("_").append(name).append("'");
        Integer codeId = 0;
        try {
            codeId = this.jdbcTemplate.queryForObject(checkColumnCodeMapping.toString(), Integer.class);
        } catch (final EmptyResultDataAccessException e) {
            LOG.debug(e.getMessage());
        }
        return ObjectUtils.defaultIfNull(codeId, 0);
    }

    private void registerColumnCodeMapping(final Map<String, Long> codeMappings) {
        if (codeMappings != null && !codeMappings.isEmpty()) {
            final String[] addSqlList = new String[codeMappings.size()];
            int i = 0;
            for (final Map.Entry<String, Long> mapEntry : codeMappings.entrySet()) {
                addSqlList[i++] = "INSERT INTO x_table_column_code_mappings (column_alias_name, code_id) VALUES ('" + mapEntry.getKey()
                        + "'," + mapEntry.getValue() + ");";
            }

            this.jdbcTemplate.batchUpdate(addSqlList);
        }
    }

    private void deleteColumnCodeMapping(final List<String> columnNames) {
        if (columnNames != null && !columnNames.isEmpty()) {
            final String[] deleteSqlList = new String[columnNames.size()];
            int i = 0;
            for (final String columnName : columnNames) {
                deleteSqlList[i++] = "DELETE FROM x_table_column_code_mappings WHERE column_alias_name='" + columnName + "';";
            }

            this.jdbcTemplate.batchUpdate(deleteSqlList);
        }
    }

    private void assertDataTableEmpty(final String datatableName) {
        final int rowCount = getRowCount(datatableName);
        if (rowCount != 0) { throw new GeneralPlatformDomainRuleException("error.msg.non.empty.datatable.cannot.be.deleted",
                "Non-empty datatable cannot be deleted."); }
    }

    private int getRowCount(final String datatableName){
        final String sql = "select count(*) from " + sqlResolver.toDefinition(datatableName) + "";
        return this.jdbcTemplate.queryForObject(sql, Integer.class);
    }

    private void validateAppTable(final String appTable) {

        if (appTable.equalsIgnoreCase("m_loan")) { return; }
        if (appTable.equalsIgnoreCase("m_savings_account")) { return; }
        if (appTable.equalsIgnoreCase("m_client")) { return; }
        if (appTable.equalsIgnoreCase("m_group")) { return; }
        if (appTable.equalsIgnoreCase("m_center")) { return; }
        if (appTable.equalsIgnoreCase("m_office")) { return; }
        if (appTable.equalsIgnoreCase("m_product_loan")) { return; }
        if (appTable.equalsIgnoreCase("m_savings_product")) { return; }

        throw new PlatformDataIntegrityException("error.msg.invalid.application.table", "Invalid Application Table: " + appTable, "name",
                appTable);
    }

    private String mapToActualAppTable(final String appTable) {
        if (appTable.equalsIgnoreCase("m_center")) {
            return "m_group";
        }
        return appTable;
    }

    private List<ResultsetRowData> fillDatatableResultSetDataRows(final String sql) {

        final SqlRowSet rs = this.jdbcTemplate.queryForRowSet(sql);

        final List<ResultsetRowData> resultsetDataRows = new ArrayList<>();

        final SqlRowSetMetaData rsmd = rs.getMetaData();

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

        return resultsetDataRows;
    }

    private String queryForApplicationTableName(final String datatable) {
        SQLInjectionValidator.validateSQLInput(datatable);
        final String sql = "SELECT application_table_name FROM x_registered_table where registered_table_name = '" + datatable + "'";

        final SqlRowSet rs = this.jdbcTemplate.queryForRowSet(sql);

        String applicationTableName = null;
        if (rs.next()) {
            applicationTableName = rs.getString("application_table_name");
        } else {
            throw new DatatableNotFoundException(datatable);
        }

        return applicationTableName;
    }

    private String getFKField(final String applicationTableName) {

        return applicationTableName.substring(2) + "_id";
    }

    /**
     * This method is used special for ppi cases Where the score need to be computed
     *
     * @param columnHeaders
     * @param datatable
     * @param fkName
     * @param appTableId
     * @param queryParams
     * @return
     */
    private String getAddSqlWithScore(final List<ResultsetColumnHeaderData> columnHeaders, final String datatable, final String fkName,
            final Long appTableId, final Map<String, String> queryParams) {

        final Map<String, String> affectedColumns = getAffectedColumns(columnHeaders, queryParams, fkName);

        String pValueWrite = "";
        String scoresId = " ";
        final String singleQuote = "'";

        String insertColumns = "";
        String selectColumns = "";
        String columnName = "";
        String pValue = null;
        for (final String key : affectedColumns.keySet()) {
            pValue = affectedColumns.get(key);

            if (Strings.isEmpty(pValue)) {
                pValueWrite = "null";
            } else {
                pValueWrite = singleQuote + this.genericDataService.replace(pValue, singleQuote, singleQuote + singleQuote) + singleQuote;

                scoresId += pValueWrite + " ,";

            }
            columnName = key;
            insertColumns += ", " + sqlResolver.toDefinition(columnName);
            selectColumns += "," + pValueWrite + " as " + sqlResolver.toDefinition(columnName);
        }

        scoresId = scoresId.replaceAll(" ,$", "");

        String vaddSql = "insert into " + sqlResolver.toDefinition(datatable) + " (" + sqlResolver.toDefinition(fkName) + " " + insertColumns + ", score )" + " select " + appTableId
                + " as id" + selectColumns + " , ( SELECT SUM( code_score ) FROM m_code_value WHERE m_code_value.id IN (" + scoresId
                + " ) ) as score";

        LOG.debug(vaddSql);

        return vaddSql;
    }

    private String getUpdateSql(List<ResultsetColumnHeaderData> columnHeaders, final String datatable, final String keyFieldName,
            final Long keyFieldValue, final Map<String, Object> changedColumns) {

        // just updating fields that have changed since pre-update read - though
        // its possible these values are different from the page the user was
        // looking at and even different from the current db values (if some
        // other update got in quick) - would need a version field for
        // completeness but its okay to take this risk with additional fields data

        if (changedColumns.size() == 0) {
            return null;
        }

        String pValue = null;
        String pValueWrite = "";
        final String singleQuote = "'";
        boolean firstColumn = true;
        String sql = "update " + sqlResolver.toDefinition(datatable) + " ";
        for (final ResultsetColumnHeaderData columnHeader : columnHeaders) {
            final String key = columnHeader.getColumnName();
            if (changedColumns.containsKey(key)) {
                if (firstColumn) {
                    sql += " set ";
                    firstColumn = false;
                } else {
                    sql += ", ";
                }

                pValue = (String) changedColumns.get(key);
                if (Strings.isEmpty(pValue)) {
                    pValueWrite = "null";
                } else {
                    JdbcJavaType columnType = columnHeader.getColumnType();
                    Boolean boolValue = columnType.canBooleanType(sqlResolver.getDialect()) ? BooleanUtils.toBooleanObject(pValue) : null;
                    if (boolValue != null) {
                        pValueWrite = sqlResolver.formatJdbcValue(columnType, sqlResolver.toJdbcValue(columnType, boolValue));
                    } else {
                        pValueWrite = singleQuote + this.genericDataService.replace(pValue, singleQuote, singleQuote + singleQuote)
                                + singleQuote;
                    }
                }
                sql += sqlResolver.toDefinition(key) + " = " + pValueWrite;
            }
        }

        sql += " where " + sqlResolver.toDefinition(keyFieldName) + " = " + keyFieldValue;

        return sql;
    }

    private Map<String, Object> getAffectedAndChangedColumns(final GenericResultsetData grs, final Map<String, String> queryParams,
            final String fkName) {

        final Map<String, String> affectedColumns = getAffectedColumns(grs.getColumnHeaders(), queryParams, fkName);
        final Map<String, Object> affectedAndChangedColumns = new HashMap<>();

        for (final String key : affectedColumns.keySet()) {
            final String columnValue = affectedColumns.get(key);
            final JdbcJavaType colType = grs.getColTypeOfColumnNamed(key);
            if (columnChanged(key, columnValue, colType, grs)) {
                affectedAndChangedColumns.put(key, columnValue);
            }
        }

        return affectedAndChangedColumns;
    }

    private boolean columnChanged(final String key, final String keyValue, final JdbcJavaType colType, final GenericResultsetData grs) {

        final List<String> columnValues = grs.getData().get(0).getRow();

        String columnValue;
        List<ResultsetColumnHeaderData> columnHeaders = grs.getColumnHeaders();
        for (int i = 0; i < columnHeaders.size(); i++) {
            ResultsetColumnHeaderData columnHeader = columnHeaders.get(i);
            if (key.equals(columnHeader.getColumnName())) {
                columnValue = columnValues.get(i);
                return notTheSame(columnValue, keyValue, colType);
            }
        }

        throw new PlatformDataIntegrityException("error.msg.invalid.columnName", "Parameter Column Name: " + key + " not found");
    }

    private Map<String, String> getAffectedColumns(final List<ResultsetColumnHeaderData> columnHeaders,
            final Map<String, String> queryParams, final String keyFieldName) {

        final String dateFormat = queryParams.get("dateFormat");
        Locale clientApplicationLocale = null;
        final String localeQueryParam = queryParams.get("locale");
        if (Strings.isNotBlank(localeQueryParam)) {
            clientApplicationLocale = new Locale(localeQueryParam);
        }

        final String underscore = "_";
        final String space = " ";
        String pValue = null;
        String queryParamColumnUnderscored;
        String columnHeaderUnderscored;
        boolean notFound;

        final Map<String, String> affectedColumns = new HashMap<>();
        final Set<String> keys = queryParams.keySet();
        for (final String key : keys) {
            // ignores id and foreign key fields
            // also ignores locale and dateformat fields that are used for
            // validating numeric and date data
            if (!(key.equalsIgnoreCase("id") || key.equalsIgnoreCase(keyFieldName) || key.equals("locale") || key.equals("dateFormat"))) {
                notFound = true;
                // matches incoming fields with and without underscores (spaces
                // and underscores considered the same)
                queryParamColumnUnderscored = this.genericDataService.replace(key, space, underscore);
                for (final ResultsetColumnHeaderData columnHeader : columnHeaders) {
                    if (notFound) {
                        columnHeaderUnderscored = this.genericDataService.replace(columnHeader.getColumnName(), space, underscore);
                        if (queryParamColumnUnderscored.equalsIgnoreCase(columnHeaderUnderscored)) {
                            pValue = queryParams.get(key);
                            pValue = validateColumn(columnHeader, pValue, dateFormat, clientApplicationLocale);
                            affectedColumns.put(columnHeader.getColumnName(), pValue);
                            notFound = false;
                        }
                    }

                }
                if (notFound) {
                    throw new PlatformDataIntegrityException("error.msg.column.not.found", "Column: " + key + " Not Found");
                }
            }
        }
        return affectedColumns;
    }

    private String validateColumn(final ResultsetColumnHeaderData columnHeader, final String pValue, final String dateFormat,
            final Locale clientApplicationLocale) {

        String paramValue = pValue;
        if (columnHeader.isDateDisplayType() || columnHeader.isDateTimeDisplayType() || columnHeader.isIntegerDisplayType()
                || columnHeader.isDecimalDisplayType() || columnHeader.isBooleanDisplayType()) {
            // only trim if string is not empty and is not null.
            paramValue = StringUtils.trim(paramValue);
        }

        if (Strings.isEmpty(paramValue) && !columnHeader.isColumnNullable()) {
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final ApiParameterError error = ApiParameterError.parameterError("error.msg.column.mandatory", "Mandatory",
                    columnHeader.getColumnName());
            dataValidationErrors.add(error);
            throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                    dataValidationErrors);
        }

        if (Strings.isNotEmpty(paramValue)) {

            if (columnHeader.hasColumnValues()) {
                if (columnHeader.isCodeValueDisplayType()) {
                    if (!columnHeader.isColumnValueAllowed(paramValue)) {
                        final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
                        final ApiParameterError error = ApiParameterError.parameterError("error.msg.invalid.columnValue",
                                "Value not found in Allowed Value list", columnHeader.getColumnName(), paramValue);
                        dataValidationErrors.add(error);
                        throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                                dataValidationErrors);
                    }

                    return paramValue;
                } else if (columnHeader.isCodeLookupDisplayType()) {

                    final Integer codeLookup = Integer.valueOf(paramValue);
                    if (!columnHeader.isColumnCodeAllowed(codeLookup)) {
                        final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
                        final ApiParameterError error = ApiParameterError.parameterError("error.msg.invalid.columnValue",
                                "Value not found in Allowed Value list", columnHeader.getColumnName(), paramValue);
                        dataValidationErrors.add(error);
                        throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                                dataValidationErrors);
                    }

                    return paramValue;
                } else {
                    throw new PlatformDataIntegrityException("error.msg.invalid.columnType.", "Code: " + columnHeader.getColumnName()
                            + " - Invalid Type " + columnHeader.getColumnType() + " (neither varchar nor int)");
                }
            }

            if (columnHeader.isDateDisplayType()) {
                final LocalDate tmpDate = JsonParserHelper.convertFrom(paramValue, columnHeader.getColumnName(), dateFormat,
                        clientApplicationLocale);
                if (tmpDate == null) {
                    paramValue = null;
                } else {
                    paramValue = tmpDate.toString();
                }
            } else if (columnHeader.isDateTimeDisplayType()) {
                final LocalDateTime tmpDateTime = JsonParserHelper.convertDateTimeFrom(paramValue, columnHeader.getColumnName(), dateFormat,
                        clientApplicationLocale);
                if (tmpDateTime == null) {
                    paramValue = null;
                } else {
                    paramValue = tmpDateTime.toString();
                }
            } else if (columnHeader.isIntegerDisplayType()) {
                final Integer tmpInt = this.helper.convertToInteger(paramValue, columnHeader.getColumnName(), clientApplicationLocale);
                if (tmpInt == null) {
                    paramValue = null;
                } else {
                    paramValue = tmpInt.toString();
                }
            } else if (columnHeader.isDecimalDisplayType()) {
                final BigDecimal tmpDecimal = this.helper.convertFrom(paramValue, columnHeader.getColumnName(), clientApplicationLocale);
                if (tmpDecimal == null) {
                    paramValue = null;
                } else {
                    paramValue = tmpDecimal.toString();
                }
            } else if (columnHeader.isBooleanDisplayType()) {

                final Boolean tmpBoolean = BooleanUtils.toBooleanObject(paramValue);
                if (tmpBoolean == null) {
                    final ApiParameterError error = ApiParameterError
                            .parameterError(
                                    "validation.msg.invalid.boolean.format", "The parameter " + columnHeader.getColumnName()
                                            + " has value: " + paramValue + " which is invalid boolean value.",
                                    columnHeader.getColumnName(), paramValue);
                    final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
                    dataValidationErrors.add(error);
                    throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                            dataValidationErrors);
                }
                paramValue = tmpBoolean.toString();
            }
            if (paramValue != null && columnHeader.hasPrecision(sqlResolver.getDialect()) && columnHeader.getColumnLength() > 0) {
                if (paramValue.length() > columnHeader.getColumnLength()) {
                    final ApiParameterError error = ApiParameterError.parameterError(
                            "validation.msg.datatable.entry.column.exceeds.maxlength",
                            "The column `" + columnHeader.getColumnName() + "` exceeds its defined max-length ",
                            columnHeader.getColumnName(), paramValue);
                    final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
                    dataValidationErrors.add(error);
                    throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                            dataValidationErrors);
                }
            }
        }

        return paramValue;
    }

    private String getDeleteEntriesSql(final String datatable, final String FKField, final Long appTableId) {
        return "delete from " + sqlResolver.toDefinition(datatable) + " where " + sqlResolver.toDefinition(FKField) + " = " + appTableId;
    }

    private String getDeleteEntrySql(final String datatable, final Long datatableId) {
        return "delete from " + sqlResolver.toDefinition(datatable) + " where id = " + datatableId;
    }

    private boolean notTheSame(final String currValue, final String pValue, final JdbcJavaType colType) {
        if (Strings.isEmpty(currValue) && Strings.isEmpty(pValue)) {
            return false;
        }

        if (Strings.isEmpty(currValue)) {
            return true;
        }

        if (Strings.isEmpty(pValue)) {
            return true;
        }

        if (colType.isDecimalType()) {
            final BigDecimal currentDecimal = BigDecimal.valueOf(Double.valueOf(currValue));
            final BigDecimal newDecimal = BigDecimal.valueOf(Double.valueOf(pValue));

            return currentDecimal.compareTo(newDecimal) != 0;
        }

        return !currValue.equals(pValue);
    }

    private boolean isDatatableAttachedToEntityDatatableCheck(final String datatableName) {
        StringBuilder builder = new StringBuilder();
        builder.append(" SELECT COUNT(edc.x_registered_table_name) FROM x_registered_table xrt ");
        builder.append(" JOIN m_entity_datatable_check edc ON edc.x_registered_table_name = xrt.registered_table_name");
        builder.append(" WHERE edc.x_registered_table_name = '" + datatableName + "'");
        final Long count = this.jdbcTemplate.queryForObject(builder.toString(), Long.class);
        return (count > 0) ? true : false;
    }

}