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
package org.apache.fineract.infrastructure.report.service;

import static org.apache.fineract.infrastructure.core.domain.FineractPlatformTenantConnection.toJdbcUrl;
import static org.apache.fineract.infrastructure.core.domain.FineractPlatformTenantConnection.toProtocol;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.fineract.infrastructure.core.api.ApiParameterHelper;
import org.apache.fineract.infrastructure.core.config.FineractProperties;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenantConnection;
import org.apache.fineract.infrastructure.core.exception.PlatformDataIntegrityException;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.core.service.database.DatabasePasswordEncryptor;
import org.apache.fineract.infrastructure.dataqueries.data.ReportExportType;
import org.apache.fineract.infrastructure.report.annotation.ReportService;
import org.apache.fineract.infrastructure.security.constants.TenantConstants;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.pentaho.reporting.engine.classic.core.ClassicEngineBoot;
import org.pentaho.reporting.engine.classic.core.CompoundDataFactory;
import org.pentaho.reporting.engine.classic.core.DataFactory;
import org.pentaho.reporting.engine.classic.core.DefaultReportEnvironment;
import org.pentaho.reporting.engine.classic.core.MasterReport;
import org.pentaho.reporting.engine.classic.core.modules.misc.datafactory.sql.DriverConnectionProvider;
import org.pentaho.reporting.engine.classic.core.modules.misc.datafactory.sql.SQLReportDataFactory;
import org.pentaho.reporting.engine.classic.core.modules.output.pageable.pdf.PdfReportUtil;
import org.pentaho.reporting.engine.classic.core.modules.output.table.csv.CSVReportUtil;
import org.pentaho.reporting.engine.classic.core.modules.output.table.html.HtmlReportUtil;
import org.pentaho.reporting.engine.classic.core.modules.output.table.xls.ExcelReportUtil;
import org.pentaho.reporting.engine.classic.core.parameters.ParameterDefinitionEntry;
import org.pentaho.reporting.libraries.resourceloader.Resource;
import org.pentaho.reporting.libraries.resourceloader.ResourceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

@Slf4j
@Service
@ReportService(type = "Pentaho")
public class PentahoReportingProcessServiceImpl implements ReportingProcessService {

    private final String mifosBaseDir = System.getProperty("user.home") + File.separator + ".mifosx";

    @Value("${FINERACT_PENTAHO_REPORTS_PATH}")
    private String fineractPentahoBaseDir;

    private final PlatformSecurityContext context;
    private final DataSource tenantDataSource;

    @Autowired
    FineractProperties fineractProperties;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    ApplicationContext contextVar;

    @Autowired
    DatabasePasswordEncryptor databasePasswordEncryptor;

    @Autowired
    public PentahoReportingProcessServiceImpl(final PlatformSecurityContext context,
            final @Qualifier("hikariTenantDataSource") DataSource tenantDataSource) {
        ClassicEngineBoot.getInstance().start();
        this.tenantDataSource = tenantDataSource;
        this.context = context;
    }

    @Override
    public Response processRequest(final String reportName, final MultivaluedMap<String, String> queryParams) {
        final var outputTypeParam = queryParams.getFirst("output-type");
        final var reportParams = getReportParams(queryParams);
        final var locale = ApiParameterHelper.extractLocale(queryParams);
        final String language = "en";
        final String dateFormat = ApiParameterHelper.extractDateFormat(queryParams);
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat, Locale.ENGLISH);

        String outputType = "HTML";
        if (StringUtils.isNotBlank(outputTypeParam)) {
            outputType = outputTypeParam;
        }

        if ((!outputType.equalsIgnoreCase("HTML") && !outputType.equalsIgnoreCase("PDF") && !outputType.equalsIgnoreCase("XLS")
                && !outputType.equalsIgnoreCase("XLSX") && !outputType.equalsIgnoreCase("CSV"))) {
            throw new PlatformDataIntegrityException("error.msg.invalid.outputType", "No matching Output Type: " + outputType);
        }

        log.debug("locale " + locale);
        log.debug("language " + language);
        final String reportPath = getReportPath() + reportName + ".prpt";
        log.info("Report path: {}", reportPath);

        // load report definition
        final ResourceManager manager = new ResourceManager();
        manager.registerDefaults();
        Resource res;

        try {
            res = manager.createDirectly(reportPath, MasterReport.class);
            final var masterReport = (MasterReport) res.getResource();

            // Override Data Connection Factory with the driver and url
            CompoundDataFactory compoundDataFactory = (CompoundDataFactory) masterReport.getDataFactory();
            setConnectionDetail(compoundDataFactory.get(0));

            final var reportEnvironment = (DefaultReportEnvironment) masterReport.getReportEnvironment();
            if (locale != null) {
                reportEnvironment.setLocale(locale);
            }

            addParametersToReport(masterReport, reportParams, simpleDateFormat);

            final var baos = new ByteArrayOutputStream();

            if ("PDF".equalsIgnoreCase(outputType)) {
                PdfReportUtil.createPDF(masterReport, baos);
                return Response.ok().entity(baos.toByteArray()).type("application/pdf").build();

            } else if ("XLS".equalsIgnoreCase(outputType)) {
                ExcelReportUtil.createXLS(masterReport, baos);
                return Response.ok().entity(baos.toByteArray()).type("application/vnd.ms-excel")
                        .header("Content-Disposition", "attachment;filename=" + reportName.replaceAll(" ", "") + ".xls").build();

            } else if ("XLSX".equalsIgnoreCase(outputType)) {
                ExcelReportUtil.createXLSX(masterReport, baos);
                return Response.ok().entity(baos.toByteArray()).type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        .header("Content-Disposition", "attachment;filename=" + reportName.replaceAll(" ", "") + ".xlsx").build();

            } else if ("CSV".equalsIgnoreCase(outputType)) {
                CSVReportUtil.createCSV(masterReport, baos, "UTF-8");
                return Response.ok().entity(baos.toByteArray()).type("text/csv")
                        .header("Content-Disposition", "attachment;filename=" + reportName.replaceAll(" ", "") + ".csv").build();

            } else if ("HTML".equalsIgnoreCase(outputType)) {
                HtmlReportUtil.createStreamHTML(masterReport, baos);
                return Response.ok().entity(baos.toByteArray()).type("text/html").build();

            } else {
                throw new PlatformDataIntegrityException("error.msg.invalid.outputType", "No matching Output Type: " + outputType);

            }
        } catch (Throwable t) {
            log.error("Pentaho failed", t);
            throw new PlatformDataIntegrityException("error.msg.reporting.error", "Pentaho failed: " + t.getMessage());
        }
    }

    private void addParametersToReport(final MasterReport report, final Map<String, String> queryParams, final SimpleDateFormat simpleDateFormat) {
        final var currentUser = this.context.authenticatedUser();
        try {
            final var rptParamValues = report.getParameterValues();
            final var paramsDefinition = report.getParameterDefinition();

            // only allow integer, long, date and string parameter types and assume all mandatory - could go more
            // detailed like Pawel did in Mifos later and could match incoming and Pentaho parameters better...
            // currently assuming they come in ok... and if not an error
            for (final ParameterDefinitionEntry paramDefEntry : paramsDefinition.getParameterDefinitions()) {
                final var paramName = paramDefEntry.getName();
                if ((!paramName.equals("tenantUrl") && (!paramName.equals("userhierarchy") && !paramName.equals("username")
                        && (!paramName.equals("password") && !paramName.equals("userid"))))) {

                    log.info("param: {}", paramName);

                    final var pValue = queryParams.get(paramName);
                    if (StringUtils.isBlank(pValue)) {
                        throw new PlatformDataIntegrityException("error.msg.reporting.error",
                                "Pentaho Parameter: " + paramName + " - not Provided");
                    }

                    final Class<?> clazz = paramDefEntry.getValueType();
                    log.info("addParametersToReport(" + paramName + " : " + pValue + " : " + clazz.getCanonicalName() + ")");

                    if (clazz.getCanonicalName().equalsIgnoreCase("java.lang.Integer")) {
                        rptParamValues.put(paramName, Integer.parseInt(pValue));
                    } else if (clazz.getCanonicalName().equalsIgnoreCase("java.lang.Long")) {
                        rptParamValues.put(paramName, Long.parseLong(pValue));
                    } else if (clazz.getCanonicalName().equalsIgnoreCase("java.sql.Date")) {
                        log.info("Param: {} {}", paramName, pValue.toString());
                        String myDate = pValue.toString();
                        Date date = simpleDateFormat.parse(myDate);
                        long millis = date.getTime();
                        java.sql.Date mySQLDate = new java.sql.Date(millis);
                        rptParamValues.put(paramName, mySQLDate);
                    } else {
                        log.info("ParamName Unknown: {} {}", clazz.getCanonicalName(), paramName);
                        log.info("ParamValue Unknown: {}", pValue.toString());
                        rptParamValues.put(paramName, pValue);
                    }
                }
            }

            // Tenant database name and current user's office hierarchy
            // passed as parameters to allow multitenant Pentaho reporting
            // and data scoping
            final var tenant = ThreadLocalContextUtil.getTenant();
            final var tenantConnection = tenant.getConnection();
            String protocol = toProtocol(this.tenantDataSource);
            Environment environment = contextVar.getEnvironment();
            String tenantUrl = toJdbcUrl(protocol, tenantConnection.getSchemaServer(), tenantConnection.getSchemaServerPort(),
                    tenantConnection.getSchemaName(), tenantConnection.getSchemaConnectionParameters());

            final var userhierarchy = currentUser.getOffice().getHierarchy();
            rptParamValues.put("userhierarchy", userhierarchy);
            final var userid = currentUser.getId();
            rptParamValues.put("userid", userid);
            rptParamValues.put("tenantUrl", tenantUrl);
            log.info("url {}", tenantUrl);
            if (tenantConnection.getSchemaUsername().equalsIgnoreCase("") || tenantConnection.getSchemaUsername() == null) {
                rptParamValues.put("username", environment.getProperty("FINERACT_DEFAULT_TENANTDB_UID"));
            } else {
                rptParamValues.put("username", tenantConnection.getSchemaUsername());
            }

            if (tenantConnection.getSchemaPassword().equalsIgnoreCase("") || tenantConnection.getSchemaPassword() == null) {
                rptParamValues.put("password", environment.getProperty("FINERACT_DEFAULT_TENANTDB_PWD"));
            } else {
                rptParamValues.put("password", databasePasswordEncryptor.decrypt(tenantConnection.getSchemaPassword()));
            }

        } catch (Throwable t) {
            log.error("error.msg.reporting.error:", t);
            throw new PlatformDataIntegrityException("error.msg.reporting.error", t.getMessage());
        }
    }

    @Override
    public Map<String, String> getReportParams(final MultivaluedMap<String, String> queryParams) {
        final Map<String, String> reportParams = new HashMap<>();
        final var keys = queryParams.keySet();
        String pKey;
        String pValue;
        for (final String k : keys) {
            if (k.startsWith("R_")) {
                pKey = k.substring(2);
                pValue = queryParams.get(k).get(0);
                reportParams.put(pKey, pValue);
            }
        }
        return reportParams;
    }

    @Override
    public List<ReportExportType> getAvailableExportTargets() {
        return new ArrayList<>();
    }

    private String getReportPath() {
        if (fineractPentahoBaseDir != null) {
            return this.fineractPentahoBaseDir + File.separator;
        }
        return this.mifosBaseDir + File.separator + "pentahoReports" + File.separator;
    }

    private void setConnectionDetail(DataFactory dataFactory) throws SQLException {
        final FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        final FineractPlatformTenantConnection tenantConnection = tenant.getConnection();
        if (!databasePasswordEncryptor.isMasterPasswordHashValid(tenantConnection.getMasterPasswordHash())) {
            throw new IllegalArgumentException("Invalid master password");
        }

        if (dataFactory instanceof SQLReportDataFactory) {
            SQLReportDataFactory sqlReportDataFactory = (SQLReportDataFactory) dataFactory;
            final DriverConnectionProvider connectionProvider = (DriverConnectionProvider) sqlReportDataFactory.getConnectionProvider();

            Driver e = DriverManager.getDriver(getTenantUrl());
            // Printing the driver
            log.info("Driver: " + e.getClass().getName().toString());

            connectionProvider.setDriver(e.getClass().getName().toString());
            connectionProvider.setUrl(getTenantUrl());
            connectionProvider.setProperty("user", tenantConnection.getSchemaUsername());

            connectionProvider.setProperty("password", databasePasswordEncryptor.decrypt(tenantConnection.getSchemaPassword()));
            sqlReportDataFactory.setConnectionProvider(connectionProvider);
        }
    }

    private String getTenantUrl() {
        final FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        final FineractPlatformTenantConnection tenantConnection = tenant.getConnection();
        String protocol = toProtocol(tenantDataSource);
        // Default properties for Writing
        String schemaServer = tenantConnection.getSchemaServer();
        String schemaPort = tenantConnection.getSchemaServerPort();
        String schemaName = tenantConnection.getSchemaName();
        String schemaUsername = tenantConnection.getSchemaUsername();
        String schemaPassword = databasePasswordEncryptor.decrypt(tenantConnection.getSchemaPassword());
        String schemaConnectionParameters = tenantConnection.getSchemaConnectionParameters();
        // Properties to ReadOnly case
        if (fineractProperties.getMode().isReadOnlyMode()) {
            schemaServer = getPropertyValue(tenantConnection.getReadOnlySchemaServer(), TenantConstants.PROPERTY_RO_SCHEMA_SERVER_NAME,
                    schemaServer);
            schemaPort = getPropertyValue(tenantConnection.getReadOnlySchemaServerPort(), TenantConstants.PROPERTY_RO_SCHEMA_SERVER_PORT,
                    schemaPort);
            schemaName = getPropertyValue(tenantConnection.getReadOnlySchemaName(), TenantConstants.PROPERTY_RO_SCHEMA_SCHEMA_NAME,
                    schemaName);
            schemaUsername = getPropertyValue(tenantConnection.getReadOnlySchemaUsername(), TenantConstants.PROPERTY_RO_SCHEMA_USERNAME,
                    schemaUsername);
            schemaPassword = getPropertyValue(tenantConnection.getReadOnlySchemaPassword(), TenantConstants.PROPERTY_RO_SCHEMA_PASSWORD,
                    schemaPassword);
            schemaConnectionParameters = getPropertyValue(tenantConnection.getReadOnlySchemaConnectionParameters(),
                    TenantConstants.PROPERTY_RO_SCHEMA_CONNECTION_PARAMETERS, schemaConnectionParameters);
        }
        String jdbcUrl = toJdbcUrl(protocol, schemaServer, schemaPort, schemaName, schemaConnectionParameters);
        log.debug("{}", jdbcUrl);

        return jdbcUrl;
    }

    private String getPropertyValue(final String baseValue, final String propertyName, final String defaultValue) {
        // If the property already has set, return It
        if (null != baseValue) {
            return baseValue;
        }
        if (applicationContext == null) {
            return defaultValue;
        }
        return applicationContext.getEnvironment().getProperty(propertyName, defaultValue);
    }
}
