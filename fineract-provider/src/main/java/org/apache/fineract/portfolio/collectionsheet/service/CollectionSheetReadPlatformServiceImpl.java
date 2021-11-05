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
package org.apache.fineract.portfolio.collectionsheet.service;

import static org.apache.fineract.portfolio.collectionsheet.CollectionSheetConstants.calendarIdParamName;
import static org.apache.fineract.portfolio.collectionsheet.CollectionSheetConstants.officeIdParamName;
import static org.apache.fineract.portfolio.collectionsheet.CollectionSheetConstants.staffIdParamName;
import static org.apache.fineract.portfolio.collectionsheet.CollectionSheetConstants.transactionDateParamName;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.fineract.infrastructure.codes.service.CodeValueReadPlatformService;
import org.apache.fineract.infrastructure.configuration.domain.ConfigurationDomainService;
import org.apache.fineract.infrastructure.core.api.JsonQuery;
import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.data.EnumOptionData;
import org.apache.fineract.infrastructure.core.domain.JdbcSupport;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.apache.fineract.organisation.monetary.data.CurrencyData;
import org.apache.fineract.portfolio.calendar.domain.Calendar;
import org.apache.fineract.portfolio.calendar.domain.CalendarEntityType;
import org.apache.fineract.portfolio.calendar.domain.CalendarInstanceRepository;
import org.apache.fineract.portfolio.calendar.domain.CalendarRepositoryWrapper;
import org.apache.fineract.portfolio.calendar.exception.NotValidRecurringDateException;
import org.apache.fineract.portfolio.calendar.service.CalendarReadPlatformService;
import org.apache.fineract.portfolio.collectionsheet.data.IndividualClientData;
import org.apache.fineract.portfolio.collectionsheet.data.IndividualCollectionSheetData;
import org.apache.fineract.portfolio.collectionsheet.data.IndividualCollectionSheetLoanFlatData;
import org.apache.fineract.portfolio.collectionsheet.data.JLGClientData;
import org.apache.fineract.portfolio.collectionsheet.data.JLGCollectionSheetData;
import org.apache.fineract.portfolio.collectionsheet.data.JLGCollectionSheetFlatData;
import org.apache.fineract.portfolio.collectionsheet.data.JLGGroupData;
import org.apache.fineract.portfolio.collectionsheet.data.LoanDueData;
import org.apache.fineract.portfolio.collectionsheet.data.SavingsDueData;
import org.apache.fineract.portfolio.collectionsheet.serialization.CollectionSheetGenerateCommandFromApiJsonDeserializer;
import org.apache.fineract.portfolio.group.data.CenterData;
import org.apache.fineract.portfolio.group.data.GroupGeneralData;
import org.apache.fineract.portfolio.group.service.CenterReadPlatformService;
import org.apache.fineract.portfolio.group.service.GroupReadPlatformService;
import org.apache.fineract.portfolio.loanproduct.data.LoanProductData;
import org.apache.fineract.portfolio.meeting.attendance.service.AttendanceDropdownReadPlatformService;
import org.apache.fineract.portfolio.meeting.attendance.service.AttendanceEnumerations;
import org.apache.fineract.portfolio.paymenttype.data.PaymentTypeData;
import org.apache.fineract.portfolio.paymenttype.service.PaymentTypeReadPlatformService;
import org.apache.fineract.portfolio.savings.data.SavingsProductData;
import org.apache.fineract.useradministration.domain.AppUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Service;

@Service
public class CollectionSheetReadPlatformServiceImpl implements CollectionSheetReadPlatformService {

    private final PlatformSecurityContext context;
    private final DataSourceSqlResolver sqlResolver;
    private final NamedParameterJdbcTemplate namedParameterjdbcTemplate;
    private final CenterReadPlatformService centerReadPlatformService;
    private final GroupReadPlatformService groupReadPlatformService;
    private final CollectionSheetGenerateCommandFromApiJsonDeserializer collectionSheetGenerateCommandFromApiJsonDeserializer;
    private final CalendarRepositoryWrapper calendarRepositoryWrapper;
    private final AttendanceDropdownReadPlatformService attendanceDropdownReadPlatformService;
    private final MandatorySavingsCollectionsheetExtractor mandatorySavingsExtractor = new MandatorySavingsCollectionsheetExtractor();
    private final CodeValueReadPlatformService codeValueReadPlatformService;
    private final PaymentTypeReadPlatformService paymentTypeReadPlatformService;
    private final CalendarReadPlatformService calendarReadPlatformService;
    private final ConfigurationDomainService configurationDomainService;
    private final CalendarInstanceRepository calendarInstanceRepository;

    @Autowired
    public CollectionSheetReadPlatformServiceImpl(final PlatformSecurityContext context, final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver,
            final CenterReadPlatformService centerReadPlatformService, final GroupReadPlatformService groupReadPlatformService,
            final CollectionSheetGenerateCommandFromApiJsonDeserializer collectionSheetGenerateCommandFromApiJsonDeserializer,
            final CalendarRepositoryWrapper calendarRepositoryWrapper,
            final AttendanceDropdownReadPlatformService attendanceDropdownReadPlatformService,
            final CodeValueReadPlatformService codeValueReadPlatformService,
            final PaymentTypeReadPlatformService paymentTypeReadPlatformService,
            final CalendarReadPlatformService calendarReadPlatformService, final ConfigurationDomainService configurationDomainService,
            final CalendarInstanceRepository calendarInstanceRepository) {
        this.context = context;
        this.centerReadPlatformService = centerReadPlatformService;
        this.namedParameterjdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.sqlResolver = sqlResolver;
        this.collectionSheetGenerateCommandFromApiJsonDeserializer = collectionSheetGenerateCommandFromApiJsonDeserializer;
        this.groupReadPlatformService = groupReadPlatformService;
        this.calendarRepositoryWrapper = calendarRepositoryWrapper;
        this.attendanceDropdownReadPlatformService = attendanceDropdownReadPlatformService;
        this.codeValueReadPlatformService = codeValueReadPlatformService;
        this.paymentTypeReadPlatformService = paymentTypeReadPlatformService;
        this.calendarReadPlatformService = calendarReadPlatformService;
        this.configurationDomainService = configurationDomainService;
        this.calendarInstanceRepository = calendarInstanceRepository;
    }

    /*
     * Reads all the loans which are due for disbursement or collection and builds hierarchical data structure for
     * collections sheet with hierarchy Groups >> Clients >> Loans.
     */
    @SuppressWarnings("null")
    private JLGCollectionSheetData buildJLGCollectionSheet(final LocalDate dueDate,
            final Collection<JLGCollectionSheetFlatData> jlgCollectionSheetFlatData) {

        boolean firstTime = true;
        Long prevGroupId = null;
        Long prevClientId = null;
        final Collection<PaymentTypeData> paymentOptions = this.paymentTypeReadPlatformService.retrieveAllPaymentTypes();

        final List<JLGGroupData> jlgGroupsData = new ArrayList<>();
        List<JLGClientData> clientsData = new ArrayList<>();
        List<LoanDueData> loansDueData = new ArrayList<>();

        JLGCollectionSheetData jlgCollectionSheetData = null;
        JLGCollectionSheetFlatData prevCollectioSheetFlatData = null;
        JLGCollectionSheetFlatData corrCollectioSheetFlatData = null;
        final Set<LoanProductData> loanProducts = new HashSet<>();
        if (jlgCollectionSheetFlatData != null) {

            for (final JLGCollectionSheetFlatData collectionSheetFlatData : jlgCollectionSheetFlatData) {

                if (collectionSheetFlatData.getProductId() != null) {
                    loanProducts.add(LoanProductData.lookupWithCurrency(collectionSheetFlatData.getProductId(),
                            collectionSheetFlatData.getProductShortName(), collectionSheetFlatData.getCurrency()));
                }
                corrCollectioSheetFlatData = collectionSheetFlatData;

                if (firstTime || collectionSheetFlatData.getGroupId().equals(prevGroupId)) {
                    if (firstTime || collectionSheetFlatData.getClientId().equals(prevClientId)) {
                        if (collectionSheetFlatData.getLoanId() != null) {
                            loansDueData.add(collectionSheetFlatData.getLoanDueData());
                        }
                    } else {
                        final JLGClientData clientData = prevCollectioSheetFlatData.getClientData();
                        clientData.setLoans(loansDueData);
                        clientsData.add(clientData);
                        loansDueData = new ArrayList<>();

                        if (collectionSheetFlatData.getLoanId() != null) {
                            loansDueData.add(collectionSheetFlatData.getLoanDueData());
                        }

                    }
                } else {

                    final JLGClientData clientData = prevCollectioSheetFlatData.getClientData();
                    clientData.setLoans(loansDueData);
                    clientsData.add(clientData);

                    final JLGGroupData jlgGroupData = prevCollectioSheetFlatData.getJLGGroupData();
                    jlgGroupData.setClients(clientsData);

                    jlgGroupsData.add(jlgGroupData);

                    loansDueData = new ArrayList<>();
                    clientsData = new ArrayList<>();

                    if (collectionSheetFlatData.getLoanId() != null) {
                        loansDueData.add(collectionSheetFlatData.getLoanDueData());
                    }
                }

                prevClientId = collectionSheetFlatData.getClientId();
                prevGroupId = collectionSheetFlatData.getGroupId();
                prevCollectioSheetFlatData = collectionSheetFlatData;
                firstTime = false;
            }

            // FIXME Need to check last loan is added under previous
            // client/group or new client / previous group or new client / new
            // group
            if (corrCollectioSheetFlatData != null) {
                final JLGClientData lastClientData = corrCollectioSheetFlatData.getClientData();
                lastClientData.setLoans(loansDueData);
                clientsData.add(lastClientData);

                final JLGGroupData jlgGroupData = corrCollectioSheetFlatData.getJLGGroupData();
                jlgGroupData.setClients(clientsData);
                jlgGroupsData.add(jlgGroupData);
            }

            jlgCollectionSheetData = JLGCollectionSheetData.instance(dueDate, loanProducts, jlgGroupsData,
                    this.attendanceDropdownReadPlatformService.retrieveAttendanceTypeOptions(), paymentOptions);
        }

        return jlgCollectionSheetData;
    }

    private final class JLGCollectionSheetFaltDataMapper implements RowMapper<JLGCollectionSheetFlatData> {

        public String collectionSheetSchema(final boolean isCenterCollection) {
            StringBuilder sql = new StringBuilder(400);
            sql.append("SELECT loandata.*, sum(lc.amount_outstanding_derived) as chargesDue from ")
                    .append("(SELECT gp.display_name As groupName, ").append("gp.id As groupId, ").append("cl.display_name As clientName, ")
                    .append("sf.id As staffId, ").append("sf.display_name As staffName, ").append("gl.id As levelId, ")
                    .append("gl.level_name As levelName, ").append("cl.id As clientId, ").append("ln.id As loanId, ")
                    .append("ln.account_no As accountId, ").append("ln.loan_status_id As accountStatusId, ")
                    .append("pl.short_name As productShortName, ").append("ln.product_id As productId, ")
                    .append("ln.currency_code as currencyCode, ln.currency_digits as currencyDigits, ln.currency_multiplesof as inMultiplesOf, rc.").append(sqlResolver.toDefinition("name")).append(" as currencyName, rc.display_symbol as currencyDisplaySymbol, rc.internationalized_name_code as currencyNameCode, ")
                    .append("(CASE WHEN ln.loan_status_id = 200  THEN  ln.principal_amount  ELSE  null END) As disbursementAmount, ")
                    .append("sum(COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.principal_amount ELSE  0.0 END), 0.0) - COALESCE((CASE WHEN ln.loan_status_id = 300 THEN  ls.principal_completed_derived ELSE  0.0 END), 0.0)) As principalDue, ")
                    .append("ln.principal_repaid_derived As principalPaid, ")
                    .append("sum(COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.interest_amount ELSE  0.0 END), 0.0) - COALESCE((CASE WHEN ln.loan_status_id = 300 THEN  ls.interest_completed_derived ELSE  0.0 END), 0.0) - COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.interest_waived_derived ELSE  0.0 END), 0.0)) As interestDue, ")
                    .append("ln.interest_repaid_derived As interestPaid, ")
                    .append("sum(COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.fee_charges_amount ELSE  0.0 END), 0.0) - COALESCE((CASE WHEN ln.loan_status_id = 300 THEN  ls.fee_charges_completed_derived ELSE  0.0 END), 0.0)) As feeDue, ")
                    .append("ln.fee_charges_repaid_derived As feePaid, ").append("ca.attendance_type_enum as attendanceTypeId ")
                    .append("FROM m_group gp ")
                    .append("LEFT JOIN m_office of ON of.id = gp.office_id AND of.hierarchy like :officeHierarchy ")
                    .append("JOIN m_group_level gl ON gl.id = gp.level_Id ").append("LEFT JOIN m_staff sf ON sf.id = gp.staff_id ")
                    .append("JOIN m_group_client gc ON gc.group_id = gp.id ").append("JOIN m_client cl ON cl.id = gc.client_id ")
                    .append("LEFT JOIN m_loan ln ON cl.id = ln.client_id  and ln.group_id=gp.id AND ln.group_id is not null AND ( ln.loan_status_id = 300 ) ")
                    .append("LEFT JOIN m_product_loan pl ON pl.id = ln.product_id ")
                    .append("LEFT JOIN m_currency rc on rc.`code` = ln.currency_code ")
                    .append("LEFT JOIN m_loan_repayment_schedule ls ON ls.loan_id = ln.id AND ls.completed_derived = ")
                    .append(sqlResolver.formatBoolValue(false))
                    .append(" AND ls.duedate <= :dueDate ")
                    .append("left join m_calendar_instance ci on gp.parent_id = ci.entity_id and ci.entity_type_enum =:entityTypeId ")
                    .append("left join m_meeting mt on ci.id = mt.calendar_instance_id and mt.meeting_date =:dueDate ")
                    .append("left join m_client_attendance ca on ca.meeting_id=mt.id and ca.client_id=cl.id ");

            if (isCenterCollection) {
                sql.append("WHERE gp.parent_id = :centerId ");
            } else {
                sql.append("WHERE gp.id = :groupId ");
            }
            sql.append("and (ln.loan_status_id != 200 AND ln.loan_status_id != 100) ");

            sql.append("and (gp.status_enum = 300 or (gp.status_enum = 600 and gp.closedon_date >= :dueDate)) ")
                    .append("and (cl.status_enum = 300 or (cl.status_enum = 600 and cl.closedon_date >= :dueDate)) ")
                    .append("GROUP BY gp.id, cl.id, ln.id, ca.attendance_type_enum ORDER BY gp.id , cl.id , ln.id ").append(") loandata ")
                    .append("LEFT JOIN m_loan_charge lc ON lc.loan_id = loandata.loanId AND lc.is_paid_derived = ")
                    .append(sqlResolver.formatBoolValue(false))
                    .append(" AND lc.is_active = ")
                    .append(sqlResolver.formatBoolValue(true))
                    .append(" AND ( lc.due_for_collection_as_of_date  <= :dueDate OR lc.charge_time_enum = 1) ")
                    .append("GROUP BY loandata.groupId, loandata.clientId, loandata.loanId ")
                    .append(", loandata.principalDue, loandata.interestDue, loandata.feeDue, loandata.attendanceTypeId ")
                    .append("ORDER BY loandata.groupId, ").append("loandata.clientId, ").append("loandata.loanId ");

            return sql.toString();

        }

        @Override
        public JLGCollectionSheetFlatData mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {

            final String groupName = rs.getString("groupName");
            final Long groupId = JdbcSupport.getLong(rs, "groupId");
            final Long staffId = JdbcSupport.getLong(rs, "staffId");
            final String staffName = rs.getString("staffName");
            final Long levelId = JdbcSupport.getLong(rs, "levelId");
            final String levelName = rs.getString("levelName");
            final String clientName = rs.getString("clientName");
            final Long clientId = JdbcSupport.getLong(rs, "clientId");
            final Long loanId = JdbcSupport.getLong(rs, "loanId");
            final String accountId = rs.getString("accountId");
            final Integer accountStatusId = JdbcSupport.getInteger(rs, "accountStatusId");
            final String productShortName = rs.getString("productShortName");
            final Long productId = JdbcSupport.getLong(rs, "productId");

            final String currencyCode = rs.getString("currencyCode");
            final String currencyName = rs.getString("currencyName");
            final String currencyNameCode = rs.getString("currencyNameCode");
            final String currencyDisplaySymbol = rs.getString("currencyDisplaySymbol");
            final Integer currencyDigits = JdbcSupport.getInteger(rs, "currencyDigits");
            final Integer inMultiplesOf = JdbcSupport.getInteger(rs, "inMultiplesOf");
            CurrencyData currencyData = null;
            if (currencyCode != null) {
                currencyData = new CurrencyData(currencyCode, currencyName, currencyDigits, inMultiplesOf, currencyDisplaySymbol,
                        currencyNameCode);
            }

            final BigDecimal disbursementAmount = rs.getBigDecimal("disbursementAmount");
            final BigDecimal principalDue = rs.getBigDecimal("principalDue");
            final BigDecimal principalPaid = rs.getBigDecimal("principalPaid");
            final BigDecimal interestDue = rs.getBigDecimal("interestDue");
            final BigDecimal interestPaid = rs.getBigDecimal("interestPaid");
            final BigDecimal chargesDue = rs.getBigDecimal("chargesDue");
            final BigDecimal feeDue = rs.getBigDecimal("feeDue");
            final BigDecimal feePaid = rs.getBigDecimal("feePaid");

            final Integer attendanceTypeId = rs.getInt("attendanceTypeId");
            final EnumOptionData attendanceType = AttendanceEnumerations.attendanceType(attendanceTypeId);

            return new JLGCollectionSheetFlatData(groupName, groupId, staffId, staffName, levelId, levelName, clientName, clientId, loanId,
                    accountId, accountStatusId, productShortName, productId, currencyData, disbursementAmount, principalDue, principalPaid,
                    interestDue, interestPaid, chargesDue, attendanceType, feeDue, feePaid);
        }

    }

    @Override
    public JLGCollectionSheetData generateGroupCollectionSheet(final Long groupId, final JsonQuery query) {

        this.collectionSheetGenerateCommandFromApiJsonDeserializer.validateForGenerateCollectionSheet(query.json());

        final Long calendarId = query.longValueOfParameterNamed(calendarIdParamName);
        final LocalDate transactionDate = query.localDateValueOfParameterNamed(transactionDateParamName);
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        final String transactionDateStr = df.format(Date.from(transactionDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));

        final Calendar calendar = this.calendarRepositoryWrapper.findOneWithNotFoundDetection(calendarId);
        // check if transaction against calendar effective from date

        final GroupGeneralData group = this.groupReadPlatformService.retrieveOne(groupId);

        // entityType should be center if it's within a center
        final CalendarEntityType entityType = group.isChildGroup() ? CalendarEntityType.CENTERS : CalendarEntityType.GROUPS;

        Long entityId = null;
        if (group.isChildGroup()) {
            entityId = group.getParentId();
        } else {
            entityId = group.getId();
        }

        Boolean isSkipMeetingOnFirstDay = false;
        Integer numberOfDays = 0;
        boolean isSkipRepaymentOnFirstMonthEnabled = this.configurationDomainService.isSkippingMeetingOnFirstDayOfMonthEnabled();
        if (isSkipRepaymentOnFirstMonthEnabled) {
            numberOfDays = this.configurationDomainService.retreivePeroidInNumberOfDaysForSkipMeetingDate().intValue();
            isSkipMeetingOnFirstDay = this.calendarReadPlatformService.isCalendarAssociatedWithEntity(entityId, calendar.getId(),
                    entityType.getValue().longValue());
        }

        if (!calendar.isValidRecurringDate(transactionDate, isSkipMeetingOnFirstDay, numberOfDays)) {
            throw new NotValidRecurringDateException("collectionsheet", "The date '" + transactionDate + "' is not a valid meeting date.",
                    transactionDate);
        }

        final AppUser currentUser = this.context.authenticatedUser();
        final String hierarchy = currentUser.getOffice().getHierarchy();
        final String officeHierarchy = hierarchy + "%";

        final JLGCollectionSheetFaltDataMapper mapper = new JLGCollectionSheetFaltDataMapper();

        final SqlParameterSource namedParameters = new MapSqlParameterSource().addValue("dueDate", transactionDateStr)
                .addValue("groupId", group.getId()).addValue("officeHierarchy", officeHierarchy)
                .addValue("entityTypeId", entityType.getValue());

        final Collection<JLGCollectionSheetFlatData> collectionSheetFlatDatas = this.namedParameterjdbcTemplate
                .query(mapper.collectionSheetSchema(false), namedParameters, mapper);

        // loan data for collection sheet
        JLGCollectionSheetData collectionSheetData = buildJLGCollectionSheet(transactionDate, collectionSheetFlatDatas);

        // mandatory savings data for collection sheet
        Collection<JLGGroupData> groupsWithSavingsData = this.namedParameterjdbcTemplate
                .query(mandatorySavingsExtractor.collectionSheetSchema(false), namedParameters, mandatorySavingsExtractor);

        // merge savings data into loan data
        mergeSavingsGroupDataIntoCollectionsheetData(groupsWithSavingsData, collectionSheetData);

        collectionSheetData = JLGCollectionSheetData.withSavingsProducts(collectionSheetData,
                retrieveSavingsProducts(groupsWithSavingsData));

        return collectionSheetData;
    }

    private void mergeSavingsGroupDataIntoCollectionsheetData(final Collection<JLGGroupData> groupsWithSavingsData,
            final JLGCollectionSheetData collectionSheetData) {
        final List<JLGGroupData> groupsWithLoanData = (List<JLGGroupData>) collectionSheetData.getGroups();
        for (JLGGroupData groupSavingsData : groupsWithSavingsData) {
            if (groupsWithLoanData.contains(groupSavingsData)) {
                mergeGroup(groupSavingsData, groupsWithLoanData);
            } else {
                groupsWithLoanData.add(groupSavingsData);
            }
        }

    }

    private void mergeGroup(final JLGGroupData groupSavingsData, final List<JLGGroupData> groupsWithLoanData) {
        final int index = groupsWithLoanData.indexOf(groupSavingsData);

        if (index < 0) {
            return;
        }

        JLGGroupData groupLoanData = groupsWithLoanData.get(index);
        List<JLGClientData> clientsLoanData = (List<JLGClientData>) groupLoanData.getClients();
        List<JLGClientData> clientsSavingsData = (List<JLGClientData>) groupSavingsData.getClients();

        for (JLGClientData clientSavingsData : clientsSavingsData) {
            if (clientsLoanData.contains(clientSavingsData)) {
                mergeClient(clientSavingsData, clientsLoanData);
            } else {
                clientsLoanData.add(clientSavingsData);
            }
        }
    }

    private void mergeClient(final JLGClientData clientSavingsData, List<JLGClientData> clientsLoanData) {
        final int index = clientsLoanData.indexOf(clientSavingsData);

        if (index < 0) {
            return;
        }

        JLGClientData clientLoanData = clientsLoanData.get(index);
        clientLoanData.setSavings(clientSavingsData.getSavings());
    }

    private Collection<SavingsProductData> retrieveSavingsProducts(Collection<JLGGroupData> groupsWithSavingsData) {
        List<SavingsProductData> savingsProducts = new ArrayList<>();
        for (JLGGroupData groupSavingsData : groupsWithSavingsData) {
            Collection<JLGClientData> clientsSavingsData = groupSavingsData.getClients();
            for (JLGClientData clientSavingsData : clientsSavingsData) {
                Collection<SavingsDueData> savingsDatas = clientSavingsData.getSavings();
                for (SavingsDueData savingsDueData : savingsDatas) {
                    final SavingsProductData savingsProduct = SavingsProductData.lookup(savingsDueData.productId(),
                            savingsDueData.productName());
                    savingsProduct.setDepositAccountType(savingsDueData.getDepositAccountType());
                    if (!savingsProducts.contains(savingsProduct)) {
                        savingsProducts.add(savingsProduct);
                    }
                }
            }
        }
        return savingsProducts;
    }

    @Override
    public JLGCollectionSheetData generateCenterCollectionSheet(final Long centerId, final JsonQuery query) {

        this.collectionSheetGenerateCommandFromApiJsonDeserializer.validateForGenerateCollectionSheet(query.json());

        final AppUser currentUser = this.context.authenticatedUser();
        final String hierarchy = currentUser.getOffice().getHierarchy();
        final String officeHierarchy = hierarchy + "%";

        final CenterData center = this.centerReadPlatformService.retrieveOne(centerId);

        final LocalDate transactionDate = query.localDateValueOfParameterNamed(transactionDateParamName);
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        final String dueDateStr = df.format(Date.from(transactionDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));

        final JLGCollectionSheetFaltDataMapper mapper = new JLGCollectionSheetFaltDataMapper();

        StringBuilder sql = new StringBuilder(mapper.collectionSheetSchema(true));

        final SqlParameterSource namedParameters = new MapSqlParameterSource().addValue("dueDate", dueDateStr)
                .addValue("centerId", center.getId()).addValue("officeHierarchy", officeHierarchy)
                .addValue("entityTypeId", CalendarEntityType.CENTERS.getValue());

        final Collection<JLGCollectionSheetFlatData> collectionSheetFlatDatas = this.namedParameterjdbcTemplate.query(sql.toString(),
                namedParameters, mapper);

        // loan data for collection sheet
        JLGCollectionSheetData collectionSheetData = buildJLGCollectionSheet(transactionDate, collectionSheetFlatDatas);

        // mandatory savings data for collection sheet
        Collection<JLGGroupData> groupsWithSavingsData = this.namedParameterjdbcTemplate
                .query(mandatorySavingsExtractor.collectionSheetSchema(true), namedParameters, mandatorySavingsExtractor);

        // merge savings data into loan data
        mergeSavingsGroupDataIntoCollectionsheetData(groupsWithSavingsData, collectionSheetData);

        collectionSheetData = JLGCollectionSheetData.withSavingsProducts(collectionSheetData,
                retrieveSavingsProducts(groupsWithSavingsData));

        return collectionSheetData;
    }

    private final class MandatorySavingsCollectionsheetExtractor implements ResultSetExtractor<Collection<JLGGroupData>> {

        private final GroupSavingsDataMapper groupSavingsDataMapper = new GroupSavingsDataMapper();

        public String collectionSheetSchema(final boolean isCenterCollection) {

            final StringBuilder sql = new StringBuilder();
            sql.append("SELECT gp.display_name As group_name, ")
                    .append("gp.id As group_id, ")
                    .append("cl.display_name As client_name, ")
                    .append("cl.id As client_id, ")
                    .append("sf.id As staff_id, ")
                    .append("sf.display_name As staff_name, ")
                    .append("gl.id As level_id, ")
                    .append("gl.level_name As level_name, ")
                    .append("sa.id As savings_id, ")
                    .append("sa.account_no As account_id, ")
                    .append("sa.status_enum As account_status_id, ")
                    .append("sp.short_name As product_short_name, ")
                    .append("sp.id As product_id, ")
                    .append("sa.currency_code as currency_code, ")
                    .append("sa.currency_digits as currency_digits, ")
                    .append("sa.currency_multiplesof as in_multiples_of, ")
                    .append("rc.").append(sqlResolver.toDefinition("name")).append(" as currency_name, ")
                    .append("rc.display_symbol as currency_display_symbol, ")
                    .append("(CASE WHEN sa.deposit_type_enum=100 THEN 'Saving Deposit' ELSE (CASE WHEN sa.deposit_type_enum=300 THEN 'Recurring Deposit' ELSE 'Current Deposit' END) END) as deposit_account_type, ")
                    .append("rc.internationalized_name_code as currency_name_code, ")
                    .append("sum(COALESCE(mss.deposit_amount,0) - COALESCE(mss.deposit_amount_completed_derived,0)) as due_amount ")

                    .append("FROM m_group gp ")
                    .append("LEFT JOIN m_office of ON of.id = gp.office_id AND of.hierarchy like :officeHierarchy ")
                    .append("JOIN m_group_level gl ON gl.id = gp.level_Id ")
                    .append("LEFT JOIN m_staff sf ON sf.id = gp.staff_id ")
                    .append("JOIN m_group_client gc ON gc.group_id = gp.id ")
                    .append("JOIN m_client cl ON cl.id = gc.client_id ")
                    .append("JOIN m_savings_account sa ON sa.client_id=cl.id and sa.status_enum=300 ")
                    .append("JOIN m_savings_product sp ON sa.product_id=sp.id ")
                    .append("LEFT JOIN m_deposit_account_recurring_detail dard ON sa.id = dard.savings_account_id")
                    .append(" AND dard.is_mandatory = ").append(sqlResolver.formatBoolValue(true))
                    .append(" AND dard.is_calendar_inherited = ").append(sqlResolver.formatBoolValue(true))
                    .append(" LEFT JOIN m_mandatory_savings_schedule mss ON mss.savings_account_id=sa.id AND mss.duedate <= :dueDate")
                    .append(" LEFT JOIN m_currency rc on rc.code = sa.currency_code ");

            if (isCenterCollection) {
                sql.append("WHERE gp.parent_id = :centerId ");
            } else {
                sql.append("WHERE gp.id = :groupId ");
            }

            sql.append("and (gp.status_enum = 300 or (gp.status_enum = 600 and gp.closedon_date >= :dueDate)) ")
                    .append("and (cl.status_enum = 300 or (cl.status_enum = 600 and cl.closedon_date >= :dueDate)) ")
                    .append("GROUP BY gp.id ,cl.id , sa.id ORDER BY gp.id , cl.id , sa.id ");

            return sql.toString();
        }

        @Override
        public Collection<JLGGroupData> extractData(ResultSet rs) throws SQLException, DataAccessException {
            List<JLGGroupData> groups = new ArrayList<>();

            JLGGroupData group = null;
            int groupIndex = 0;
            boolean isEndOfRecords = false;
            // move cursor to first row.
            final boolean isNotEmtyResultSet = rs.next();

            if (isNotEmtyResultSet) {
                while (!isEndOfRecords) {
                    group = groupSavingsDataMapper.mapRowData(rs, groupIndex++);
                    groups.add(group);
                    isEndOfRecords = rs.isAfterLast();
                }
            }

            return groups;
        }
    }

    private static final class GroupSavingsDataMapper implements RowMapper<JLGGroupData> {

        private final ClientSavingsDataMapper clientSavingsDataMapper = new ClientSavingsDataMapper();

        private GroupSavingsDataMapper() {}

        public JLGGroupData mapRowData(ResultSet rs, int rowNum) throws SQLException {
            final List<JLGClientData> clients = new ArrayList<>();
            final JLGGroupData group = this.mapRow(rs, rowNum);
            final Long previousGroupId = group.getGroupId();

            // first client row of new group
            JLGClientData client = clientSavingsDataMapper.mapRowData(rs, rowNum);
            clients.add(client);

            // if its not after last row loop
            while (!rs.isAfterLast()) {
                final Long groupId = JdbcSupport.getLong(rs, "groupId");
                if (previousGroupId != null && groupId.compareTo(previousGroupId) != 0) {
                    // return for next group details
                    return JLGGroupData.withClients(group, clients);
                }
                client = clientSavingsDataMapper.mapRowData(rs, rowNum);
                clients.add(client);
            }

            return JLGGroupData.withClients(group, clients);
        }

        @Override
        public JLGGroupData mapRow(ResultSet rs, @SuppressWarnings("unused") int rowNum) throws SQLException {

            final String groupName = rs.getString("groupName");
            final Long groupId = JdbcSupport.getLong(rs, "groupId");
            final Long staffId = JdbcSupport.getLong(rs, "staffId");
            final String staffName = rs.getString("staffName");
            final Long levelId = JdbcSupport.getLong(rs, "levelId");
            final String levelName = rs.getString("levelName");
            return JLGGroupData.instance(groupId, groupName, staffId, staffName, levelId, levelName);
        }
    }

    private static final class ClientSavingsDataMapper implements RowMapper<JLGClientData> {

        private final SavingsDueDataMapper savingsDueDataMapper = new SavingsDueDataMapper();

        private ClientSavingsDataMapper() {}

        public JLGClientData mapRowData(ResultSet rs, int rowNum) throws SQLException {

            List<SavingsDueData> savings = new ArrayList<>();

            JLGClientData client = this.mapRow(rs, rowNum);
            final Long previousClientId = client.getClientId();

            // first savings row of new client record
            SavingsDueData saving = savingsDueDataMapper.mapRow(rs, rowNum);
            savings.add(saving);

            while (rs.next()) {
                final Long clientId = JdbcSupport.getLong(rs, "clientId");
                if (previousClientId != null && clientId.compareTo(previousClientId) != 0) {
                    // client id changes then return for next client data
                    return JLGClientData.withSavings(client, savings);
                }
                saving = savingsDueDataMapper.mapRow(rs, rowNum);
                savings.add(saving);
            }
            return JLGClientData.withSavings(client, savings);
        }

        @Override
        public JLGClientData mapRow(ResultSet rs, @SuppressWarnings("unused") int rowNum) throws SQLException {

            final String clientName = rs.getString("clientName");
            final Long clientId = JdbcSupport.getLong(rs, "clientId");
            // final Integer attendanceTypeId = rs.getInt("attendanceTypeId");
            // final EnumOptionData attendanceType =
            // AttendanceEnumerations.attendanceType(attendanceTypeId);
            final EnumOptionData attendanceType = null;

            return JLGClientData.instance(clientId, clientName, attendanceType);
        }
    }

    private static final class SavingsDueDataMapper implements RowMapper<SavingsDueData> {

        private SavingsDueDataMapper() {}

        @Override
        public SavingsDueData mapRow(ResultSet rs, int rowNum) throws SQLException {
            final Long savingsId = rs.getLong("savingsId");
            final String accountId = rs.getString("accountId");
            final Integer accountStatusId = JdbcSupport.getInteger(rs, "accountStatusId");
            final String productName = rs.getString("productShortName");
            final Long productId = rs.getLong("productId");
            final BigDecimal dueAmount = rs.getBigDecimal("dueAmount");
            final String currencyCode = rs.getString("currencyCode");
            final String currencyName = rs.getString("currencyName");
            final String currencyNameCode = rs.getString("currencyNameCode");
            final String currencyDisplaySymbol = rs.getString("currencyDisplaySymbol");
            final Integer currencyDigits = JdbcSupport.getInteger(rs, "currencyDigits");
            final Integer inMultiplesOf = JdbcSupport.getInteger(rs, "inMultiplesOf");
            final String depositAccountType = rs.getString("depositAccountType");
            // currency
            final CurrencyData currency = new CurrencyData(currencyCode, currencyName, currencyDigits, inMultiplesOf, currencyDisplaySymbol,
                    currencyNameCode);

            return SavingsDueData.instance(savingsId, accountId, accountStatusId, productName, productId, currency, dueAmount,
                    depositAccountType);
        }
    }

    @Override
    public IndividualCollectionSheetData generateIndividualCollectionSheet(final JsonQuery query) {

        this.collectionSheetGenerateCommandFromApiJsonDeserializer.validateForGenerateCollectionSheetOfIndividuals(query.json());

        final LocalDate transactionDate = query.localDateValueOfParameterNamed(transactionDateParamName);
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        final String transactionDateStr = df.format(Date.from(transactionDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));

        final AppUser currentUser = this.context.authenticatedUser();
        final String hierarchy = currentUser.getOffice().getHierarchy();
        final String officeHierarchy = hierarchy + "%";

        final Long officeId = query.longValueOfParameterNamed(officeIdParamName);
        final Long staffId = query.longValueOfParameterNamed(staffIdParamName);
        final boolean checkForOfficeId = officeId != null;
        final boolean checkForStaffId = staffId != null;

        final IndividualCollectionSheetFaltDataMapper mapper = new IndividualCollectionSheetFaltDataMapper(checkForOfficeId,
                checkForStaffId);

        final SqlParameterSource namedParameters = new MapSqlParameterSource().addValue("dueDate", transactionDateStr)
                .addValue("officeHierarchy", officeHierarchy);

        if (checkForOfficeId) {
            ((MapSqlParameterSource) namedParameters).addValue("officeId", officeId);
        }
        if (checkForStaffId) {
            ((MapSqlParameterSource) namedParameters).addValue("staffId", staffId);
        }

        final Collection<IndividualCollectionSheetLoanFlatData> collectionSheetFlatDatas = this.namedParameterjdbcTemplate
                .query(mapper.sqlSchema(), namedParameters, mapper);

        IndividualMandatorySavingsCollectionsheetExtractor mandatorySavingsExtractor = new IndividualMandatorySavingsCollectionsheetExtractor(
                checkForOfficeId, checkForStaffId);
        // mandatory savings data for collection sheet
        Collection<IndividualClientData> clientData = this.namedParameterjdbcTemplate
                .query(mandatorySavingsExtractor.collectionSheetSchema(), namedParameters, mandatorySavingsExtractor);

        // merge savings data into loan data
        mergeLoanData(collectionSheetFlatDatas, (List<IndividualClientData>) clientData);

        final Collection<PaymentTypeData> paymentOptions = this.paymentTypeReadPlatformService.retrieveAllPaymentTypes();

        return IndividualCollectionSheetData.instance(transactionDate, clientData, paymentOptions);

    }

    private final class IndividualCollectionSheetFaltDataMapper implements RowMapper<IndividualCollectionSheetLoanFlatData> {

        private final String sql;

        public IndividualCollectionSheetFaltDataMapper(final boolean checkForOfficeId, final boolean checkforStaffId) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT loandata.*, sum(lc.amount_outstanding_derived) as charges_due ");
            sb.append("from (SELECT cl.display_name As client_name, ");
            sb.append("cl.id As client_id, ln.id As loan_id, ln.account_no As account_id, ln.loan_status_id As account_status_id,");
            sb.append(" pl.short_name As product_short_name, ln.product_id As product_id, ");
            sb.append("ln.currency_code as currency_code, ln.currency_digits as currency_digits, ln.currency_multiplesof as in_multiples_of, ");
            sb.append("rc.").append(sqlResolver.toDefinition("name")).append(" as currency_name, rc.display_symbol as currency_display_symbol, rc.internationalized_name_code as currency_name_code, ");
            sb.append("(CASE WHEN ln.loan_status_id = 200 THEN ln.principal_amount ELSE null END) As disbursement_amount, ");
            sb.append("sum(COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.principal_amount ELSE 0.0 END), 0.0) - COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.principal_completed_derived ELSE 0.0 END), 0.0)) As principal_due, ");
            sb.append("ln.principal_repaid_derived As principal_paid, ");
            sb.append("sum(COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.interest_amount ELSE 0.0 END), 0.0) - COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.interest_completed_derived ELSE 0.0 END), 0.0)) As interest_due, ");
            sb.append("ln.interest_repaid_derived As interest_paid, ");
            sb.append("sum(COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.fee_charges_amount ELSE 0.0 END), 0.0) - COALESCE((CASE WHEN ln.loan_status_id = 300 THEN ls.fee_charges_completed_derived ELSE 0.0 END), 0.0)) As fee_due, ");
            sb.append("ln.fee_charges_repaid_derived As fee_paid ");
            sb.append("FROM m_loan ln ");
            sb.append("JOIN m_client cl ON cl.id = ln.client_id  ");
            sb.append("LEFT JOIN m_office of ON of.id = cl.office_id  AND of.hierarchy like :officeHierarchy ");
            sb.append("LEFT JOIN m_product_loan pl ON pl.id = ln.product_id ");
            sb.append("LEFT JOIN m_currency rc on rc.code = ln.currency_code ");
            sb.append("JOIN m_loan_repayment_schedule ls ON ls.loan_id = ln.id AND ls.completed_derived = ")
                    .append(sqlResolver.formatBoolValue(false)).append(" AND ls.duedate <= :dueDate ");
            sb.append("where ");
            if (checkForOfficeId) {
                sb.append("of.id = :officeId and ");
            }
            if (checkforStaffId) {
                sb.append("ln.loan_officer_id = :staffId and ");
            }
            sb.append("(ln.loan_status_id = 300) ");
            sb.append("and ln.group_id is null GROUP BY cl.id , ln.id ORDER BY cl.id , ln.id ) loandata ");
            sb.append("LEFT JOIN m_loan_charge lc ON lc.loan_id = loandata.loan_id")
                    .append(" AND lc.is_paid_derived = ").append(sqlResolver.formatBoolValue(false))
                    .append(" AND lc.is_active = ").append(sqlResolver.formatBoolValue(true))
                    .append(" AND ( lc.due_for_collection_as_of_date  <= :dueDate OR lc.charge_time_enum = 1) ");
            sb.append("GROUP BY loandata.client_id, loandata.loan_id ORDER BY loandata.client_id, loandata.loan_id ");

            sql = sb.toString();
        }

        public String sqlSchema() {
            return sql;
        }

        @Override
        public IndividualCollectionSheetLoanFlatData mapRow(ResultSet rs, @SuppressWarnings("unused") int rowNum) throws SQLException {
            final String clientName = rs.getString("client_name");
            final Long clientId = JdbcSupport.getLong(rs, "client_id");
            final Long loanId = JdbcSupport.getLong(rs, "loan_id");
            final String accountId = rs.getString("account_id");
            final Integer accountStatusId = JdbcSupport.getInteger(rs, "account_status_id");
            final String productShortName = rs.getString("product_short_name");
            final Long productId = JdbcSupport.getLong(rs, "product_id");

            final String currencyCode = rs.getString("currency_code");
            final String currencyName = rs.getString("currency_name");
            final String currencyNameCode = rs.getString("currency_name_code");
            final String currencyDisplaySymbol = rs.getString("currency_display_symbol");
            final Integer currencyDigits = JdbcSupport.getInteger(rs, "currency_digits");
            final Integer inMultiplesOf = JdbcSupport.getInteger(rs, "in_multiples_of");
            CurrencyData currencyData = null;
            if (currencyCode != null) {
                currencyData = new CurrencyData(currencyCode, currencyName, currencyDigits, inMultiplesOf, currencyDisplaySymbol,
                        currencyNameCode);
            }

            final BigDecimal disbursementAmount = rs.getBigDecimal("disbursement_amount");
            final BigDecimal principalDue = rs.getBigDecimal("principal_due");
            final BigDecimal principalPaid = rs.getBigDecimal("principal_paid");
            final BigDecimal interestDue = rs.getBigDecimal("interest_due");
            final BigDecimal interestPaid = rs.getBigDecimal("interest_paid");
            final BigDecimal chargesDue = rs.getBigDecimal("charges_due");
            final BigDecimal feeDue = rs.getBigDecimal("fee_due");
            final BigDecimal feePaid = rs.getBigDecimal("fee_paid");

            return new IndividualCollectionSheetLoanFlatData(clientName, clientId, loanId, accountId, accountStatusId, productShortName,
                    productId, currencyData, disbursementAmount, principalDue, principalPaid, interestDue, interestPaid, chargesDue,
                    feeDue, feePaid);
        }
    }

    private final class IndividualMandatorySavingsCollectionsheetExtractor implements ResultSetExtractor<Collection<IndividualClientData>> {

        private final SavingsDueDataMapper savingsDueDataMapper = new SavingsDueDataMapper();

        private final String sql;

        public IndividualMandatorySavingsCollectionsheetExtractor(final boolean checkForOfficeId, final boolean checkforStaffId) {

            final StringBuilder sb = new StringBuilder();
            sb.append("SELECT (CASE WHEN sa.deposit_type_enum=100 THEN 'Saving Deposit' ELSE (CASE WHEN sa.deposit_type_enum=300 THEN 'Recurring Deposit' ELSE 'Current Deposit' END) END) as deposit_account_type, cl.display_name As client_name, cl.id As client_id, ");
            sb.append("sa.id As savings_id, sa.account_no As account_id, sa.status_enum As account_status_id, ");
            sb.append("sp.short_name As product_short_name, sp.id As product_id, ");
            sb.append("sa.currency_code as currency_code, sa.currency_digits as currency_digits, sa.currency_multiplesof as in_multiples_of, ");
            sb.append("rc.").append(sqlResolver.toDefinition("name")).append(" as currency_name, rc.display_symbol as currency_display_symbol, rc.internationalized_name_code as currency_name_code, ");
            sb.append("sum(COALESCE(mss.deposit_amount,0) - COALESCE(mss.deposit_amount_completed_derived,0)) as due_amount ");
            sb.append("FROM m_savings_account sa ");
            sb.append("JOIN m_client cl ON cl.id = sa.client_id ");
            sb.append("JOIN m_savings_product sp ON sa.product_id=sp.id ");
            sb.append("LEFT JOIN m_deposit_account_recurring_detail dard ON sa.id = dard.savings_account_id")
                    .append(" AND dard.is_mandatory = ").append(sqlResolver.formatBoolValue(true))
                    .append(" AND dard.is_calendar_inherited = ").append(sqlResolver.formatBoolValue(false));
            sb.append(" LEFT JOIN m_mandatory_savings_schedule mss ON mss.savings_account_id=sa.id AND mss.completed_derived = ")
                    .append(sqlResolver.formatBoolValue(false)).append("AND mss.duedate <= :dueDate ");
            sb.append("LEFT JOIN m_office of ON of.id = cl.office_id AND of.hierarchy like :officeHierarchy ");
            sb.append("LEFT JOIN m_currency rc on rc.code = sa.currency_code ");
            sb.append("WHERE sa.status_enum=300 and sa.group_id is null and sa.deposit_type_enum in (100,300,400) ");
            sb.append("and (cl.status_enum = 300 or (cl.status_enum = 600 and cl.closedon_date >= :dueDate)) ");
            if (checkForOfficeId) {
                sb.append("and of.id = :officeId ");
            }
            if (checkforStaffId) {
                sb.append("and sa.field_officer_id = :staffId ");
            }
            sb.append("GROUP BY cl.id , sa.id ORDER BY cl.id , sa.id ");

            this.sql = sb.toString();
        }

        public String collectionSheetSchema() {
            return this.sql;
        }

        @SuppressWarnings("null")
        @Override
        public Collection<IndividualClientData> extractData(ResultSet rs) throws SQLException, DataAccessException {
            List<IndividualClientData> clientData = new ArrayList<>();
            int rowNum = 0;

            IndividualClientData client = null;
            Long previousClientId = null;

            while (rs.next()) {
                final Long clientId = JdbcSupport.getLong(rs, "client_id");
                if (previousClientId == null || clientId.compareTo(previousClientId) != 0) {
                    final String clientName = rs.getString("client_name");
                    client = IndividualClientData.instance(clientId, clientName);
                    client = IndividualClientData.withSavings(client, new ArrayList<>());
                    clientData.add(client);
                    previousClientId = clientId;
                }
                SavingsDueData saving = savingsDueDataMapper.mapRow(rs, rowNum);
                client.addSavings(saving);
                rowNum++;
            }

            return clientData;
        }
    }

    private void mergeLoanData(final Collection<IndividualCollectionSheetLoanFlatData> loanFlatDatas,
            List<IndividualClientData> clientDatas) {

        IndividualClientData clientSavingsData = null;
        for (IndividualCollectionSheetLoanFlatData loanFlatData : loanFlatDatas) {
            IndividualClientData clientData = loanFlatData.getClientData();
            if (clientSavingsData == null || !clientSavingsData.equals(clientData)) {
                if (clientDatas.contains(clientData)) {
                    final int index = clientDatas.indexOf(clientData);
                    if (index < 0) {
                        return;
                    }
                    clientSavingsData = clientDatas.get(index);
                    clientSavingsData.setLoans(new ArrayList<LoanDueData>());
                } else {
                    clientSavingsData = clientData;
                    clientSavingsData.setLoans(new ArrayList<LoanDueData>());
                    clientDatas.add(clientSavingsData);
                }
            }
            clientSavingsData.addLoans(loanFlatData.getLoanDueData());
        }
    }
}
