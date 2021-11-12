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
package org.apache.fineract.accounting.journalentry.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.fineract.accounting.common.AccountingEnumerations;
import org.apache.fineract.accounting.financialactivityaccount.domain.FinancialActivityAccount;
import org.apache.fineract.accounting.financialactivityaccount.domain.FinancialActivityAccountRepositoryWrapper;
import org.apache.fineract.accounting.glaccount.data.GLAccountData;
import org.apache.fineract.accounting.glaccount.domain.GLAccountType;
import org.apache.fineract.accounting.glaccount.service.GLAccountReadPlatformService;
import org.apache.fineract.accounting.journalentry.data.JournalEntryAssociationParametersData;
import org.apache.fineract.accounting.journalentry.data.JournalEntryData;
import org.apache.fineract.accounting.journalentry.data.OfficeOpeningBalancesData;
import org.apache.fineract.accounting.journalentry.data.TransactionDetailData;
import org.apache.fineract.accounting.journalentry.data.TransactionTypeEnumData;
import org.apache.fineract.accounting.journalentry.exception.JournalEntriesNotFoundException;
import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.data.EnumOptionData;
import org.apache.fineract.infrastructure.core.domain.JdbcSupport;
import org.apache.fineract.infrastructure.core.exception.GeneralPlatformDomainRuleException;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.core.service.Page;
import org.apache.fineract.infrastructure.core.service.PaginationHelper;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.infrastructure.core.service.SearchParameters;
import org.apache.fineract.infrastructure.security.utils.ColumnValidator;
import org.apache.fineract.organisation.monetary.data.CurrencyData;
import org.apache.fineract.organisation.office.data.OfficeData;
import org.apache.fineract.organisation.office.service.OfficeReadPlatformService;
import org.apache.fineract.portfolio.account.PortfolioAccountType;
import org.apache.fineract.portfolio.loanaccount.data.LoanTransactionEnumData;
import org.apache.fineract.portfolio.loanproduct.service.LoanEnumerations;
import org.apache.fineract.portfolio.note.data.NoteData;
import org.apache.fineract.portfolio.paymentdetail.data.PaymentDetailData;
import org.apache.fineract.portfolio.paymenttype.data.PaymentTypeData;
import org.apache.fineract.portfolio.savings.data.SavingsAccountTransactionEnumData;
import org.apache.fineract.portfolio.savings.service.SavingsEnumerations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Service
public class JournalEntryReadPlatformServiceImpl implements JournalEntryReadPlatformService {

    private final JdbcTemplate jdbcTemplate;
    private final DataSourceSqlResolver sqlResolver;
    private final GLAccountReadPlatformService glAccountReadPlatformService;
    private final OfficeReadPlatformService officeReadPlatformService;
    private final ColumnValidator columnValidator;
    private final FinancialActivityAccountRepositoryWrapper financialActivityAccountRepositoryWrapper;

    private final PaginationHelper<JournalEntryData> paginationHelper = new PaginationHelper<>();

    @Autowired
    public JournalEntryReadPlatformServiceImpl(final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver,
            final GLAccountReadPlatformService glAccountReadPlatformService, final ColumnValidator columnValidator,
            final OfficeReadPlatformService officeReadPlatformService,
            final FinancialActivityAccountRepositoryWrapper financialActivityAccountRepositoryWrapper) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.sqlResolver = sqlResolver;
        this.glAccountReadPlatformService = glAccountReadPlatformService;
        this.officeReadPlatformService = officeReadPlatformService;
        this.financialActivityAccountRepositoryWrapper = financialActivityAccountRepositoryWrapper;
        this.columnValidator = columnValidator;
    }

    private static final class GLJournalEntryMapper implements RowMapper<JournalEntryData> {

        private final JournalEntryAssociationParametersData associationParametersData;
        private static final String FROM = " from acc_gl_journal_entry as journal_entry " +
                " left join acc_gl_account as gl_account on gl_account.id = journal_entry.account_id" +
                    " left join m_office as office on office.id = journal_entry.office_id" +
                    " left join m_appuser as creating_user on creating_user.id = journal_entry.createdby_id " +
                    " join m_currency curr on curr.code = journal_entry.currency_code ";

        public GLJournalEntryMapper(final JournalEntryAssociationParametersData associationParametersData) {
            if (associationParametersData == null) {
                this.associationParametersData = new JournalEntryAssociationParametersData();
            } else {
                this.associationParametersData = associationParametersData;
            }
        }

        public String schema() {
            StringBuilder sb = new StringBuilder();
            sb.append(" journal_entry.id as id, gl_account.classification_enum as classification ,")
                    .append("journal_entry.transaction_id,")
                    .append(" gl_account.name as gl_account_name, gl_account.gl_code as gl_account_code,gl_account.id as gl_account_id, ")
                    .append(" journal_entry.office_id as office_id, office.name as office_name, journal_entry.ref_num as reference_number, ")
                    .append(" journal_entry.manual_entry as manual_entry,journal_entry.transaction_date as transaction_date, journal_entry.entry_date as entry_date, ")
                    .append(" journal_entry.type_enum as entry_type,journal_entry.amount as amount, journal_entry.transaction_id as transaction_id,")
                    .append(" journal_entry.entity_type_enum as entity_type, journal_entry.entity_id as entity_id, creating_user.id as created_by_user_id, ")
                    .append(" creating_user.username as created_by_user_name, journal_entry.description as comments, ")
                    .append(" journal_entry.created_date as created_date, journal_entry.reversed as reversed, ")
                    .append(" journal_entry.currency_code as currency_code, curr.name as currency_name, curr.internationalized_name_code as currency_name_code, ")
                    .append(" curr.display_symbol as currency_display_symbol, curr.decimal_places as currency_digits, curr.currency_multiplesof as in_multiples_of ");
            if (associationParametersData.isRunningBalanceRequired()) {
                sb.append(" ,journal_entry.is_running_balance_calculated as running_balance_computed, ")
                        .append(" journal_entry.office_running_balance as office_running_balance, ")
                        .append(" journal_entry.organization_running_balance as organization_running_balance ");
            }
            if (associationParametersData.isTransactionDetailsRequired()) {
                sb.append(" ,pd.receipt_number as receipt_number, ").append(" pd.check_number as check_number, ")
                        .append(" pd.account_number as account_number, ").append(" pt.value as payment_type_name, ")
                        .append(" pd.payment_type_id as payment_type_id,").append(" pd.bank_number as bank_number, ")
                        .append(" pd.routing_code as routing_code, ").append(" note.id as note_id, ")
                        .append(" note.note as transaction_note, ").append(" lt.transaction_type_enum as loan_transaction_type, ")
                        .append(" st.transaction_type_enum as savings_transaction_type ");
            }
            sb.append(FROM);
            if (associationParametersData.isTransactionDetailsRequired()) {
                sb.append(" left join m_loan_transaction as lt on journal_entry.loan_transaction_id = lt.id ")
                        .append(" left join m_savings_account_transaction as st on journal_entry.savings_transaction_id = st.id ")
                        .append(" left join m_payment_detail as pd on lt.payment_detail_id = pd.id or st.payment_detail_id = pd.id or journal_entry.payment_details_id = pd.id")
                        .append(" left join m_payment_type as pt on pt.id = pd.payment_type_id ")
                        .append(" left join m_note as note on lt.id = note.loan_transaction_id or st.id = note.savings_account_transaction_id ");
            }
            return sb.toString();

        }

        public String countSchema() {
            return " count(journal_entry.*) " + FROM;
        }

        @Override
        public JournalEntryData mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {

            final Long id = rs.getLong("id");
            final Long officeId = rs.getLong("office_id");
            final String officeName = rs.getString("office_name");
            final String glCode = rs.getString("gl_account_code");
            final String glAccountName = rs.getString("gl_account_name");
            final Long glAccountId = rs.getLong("gl_account_id");
            final int accountTypeId = JdbcSupport.getInteger(rs, "classification");
            final EnumOptionData accountType = AccountingEnumerations.gLAccountType(accountTypeId);
            final LocalDate transactionDate = JdbcSupport.getLocalDate(rs, "transaction_date");
            final Boolean manualEntry = rs.getBoolean("manual_entry");
            final BigDecimal amount = rs.getBigDecimal("amount");
            final int entryTypeId = JdbcSupport.getInteger(rs, "entry_type");
            final EnumOptionData entryType = AccountingEnumerations.journalEntryType(entryTypeId);
            final String transactionId = rs.getString("transaction_id");
            final Integer entityTypeId = JdbcSupport.getInteger(rs, "entity_type");
            final LocalDate entryDate = JdbcSupport.getLocalDate(rs,"entry_date");
            EnumOptionData entityType = null;
            if (entityTypeId != null) {
                entityType = AccountingEnumerations.portfolioProductType(entityTypeId);

            }

            final Long entityId = JdbcSupport.getLong(rs, "entity_id");
            final Long createdByUserId = rs.getLong("created_by_user_id");
            final LocalDate createdDate = JdbcSupport.getLocalDate(rs, "created_date");
            final String createdByUserName = rs.getString("created_by_user_name");
            final String comments = rs.getString("comments");
            final Boolean reversed = rs.getBoolean("reversed");
            final String referenceNumber = rs.getString("reference_number");
            BigDecimal officeRunningBalance = null;
            BigDecimal organizationRunningBalance = null;
            Boolean runningBalanceComputed = null;

            final String currencyCode = rs.getString("currency_code");
            final String currencyName = rs.getString("currency_name");
            final String currencyNameCode = rs.getString("currency_name_code");
            final String currencyDisplaySymbol = rs.getString("currency_display_symbol");
            final Integer currencyDigits = JdbcSupport.getInteger(rs, "currency_digits");
            final Integer inMultiplesOf = JdbcSupport.getInteger(rs, "in_multiples_of");
            final CurrencyData currency = new CurrencyData(currencyCode, currencyName, currencyDigits, inMultiplesOf, currencyDisplaySymbol,
                    currencyNameCode);

            if (associationParametersData.isRunningBalanceRequired()) {
                officeRunningBalance = rs.getBigDecimal("office_running_balance");
                organizationRunningBalance = rs.getBigDecimal("organization_running_balance");
                runningBalanceComputed = rs.getBoolean("running_balance_computed");
            }
            TransactionDetailData transactionDetailData = null;

            if (associationParametersData.isTransactionDetailsRequired()) {
                PaymentDetailData paymentDetailData = null;
                final Long paymentTypeId = JdbcSupport.getLong(rs, "payment_type_id");
                if (paymentTypeId != null) {
                    final String typeName = rs.getString("payment_type_name");
                    final PaymentTypeData paymentType = PaymentTypeData.instance(paymentTypeId, typeName);
                    final String accountNumber = rs.getString("account_number");
                    final String checkNumber = rs.getString("check_number");
                    final String routingCode = rs.getString("routing_code");
                    final String receiptNumber = rs.getString("receipt_number");
                    final String bankNumber = rs.getString("bank_number");
                    paymentDetailData = new PaymentDetailData(id, paymentType, accountNumber, checkNumber, routingCode, receiptNumber,
                            bankNumber);
                }
                NoteData noteData = null;
                final Long noteId = JdbcSupport.getLong(rs, "note_id");
                if (noteId != null) {
                    final String note = rs.getString("transaction_note");
                    noteData = new NoteData(noteId, null, null, null, null, null, null, null, note, null, null, null, null, null, null);
                }
                Long transaction = null;
                if (entityType != null) {
                    transaction = Long.parseLong(transactionId.substring(1).trim());
                }

                TransactionTypeEnumData transactionTypeEnumData = null;

                if (PortfolioAccountType.fromInt(entityTypeId).isLoanAccount()) {
                    final LoanTransactionEnumData loanTransactionType = LoanEnumerations.transactionType(JdbcSupport.getInteger(rs,
                            "loan_transaction_type"));
                    transactionTypeEnumData = new TransactionTypeEnumData(loanTransactionType.id(), loanTransactionType.getCode(),
                            loanTransactionType.getValue());
                } else if (PortfolioAccountType.fromInt(entityTypeId).isSavingsAccount()) {
                    final SavingsAccountTransactionEnumData savingsTransactionType = SavingsEnumerations.transactionType(JdbcSupport
                            .getInteger(rs, "savings_transaction_type"));
                    transactionTypeEnumData = new TransactionTypeEnumData(savingsTransactionType.getId(), savingsTransactionType.getCode(),
                            savingsTransactionType.getValue());
                }

                transactionDetailData = new TransactionDetailData(transaction, paymentDetailData, noteData, transactionTypeEnumData);
            }
            return new JournalEntryData(id, officeId, officeName, glAccountName, glAccountId, glCode, accountType, transactionDate,
                    entryType, amount, transactionId, manualEntry, entityType, entityId, createdByUserId, createdDate, createdByUserName,
                    comments, reversed, referenceNumber, officeRunningBalance, organizationRunningBalance, runningBalanceComputed,
                    transactionDetailData, currency);
        }
    }

    @Override
    public Page<JournalEntryData> retrieveAll(final SearchParameters searchParameters, final Long glAccountId,
                                              final Boolean onlyManualEntries, final Date fromDate, final Date toDate, final String transactionId,
                                              final Integer entityType,
                                              final JournalEntryAssociationParametersData associationParametersData) {
        GLJournalEntryMapper rm = new GLJournalEntryMapper(associationParametersData);
        boolean mySql = sqlResolver.getDialect().isMySql();
        final StringBuilder sqlBuilder = new StringBuilder("select ");
        if (mySql) {
            sqlBuilder.append("SQL_CALC_FOUND_ROWS ");
        }
        sqlBuilder.append(rm.schema());

        final Object[] objectArray = new Object[15];
        int arrayPos = 0;
        StringBuilder whereClause = new StringBuilder();

        if (StringUtils.isNotBlank(transactionId)) {
            whereClause.append(" where ");

            whereClause.append(" journal_entry.transaction_id = ?");
            objectArray[arrayPos] = transactionId;
            arrayPos = arrayPos + 1;
        }

        if (entityType != null && entityType != 0 && (onlyManualEntries == null)) {
            whereClause.append(whereClause.length() == 0 ? " where " : " and ");

            whereClause.append(" journal_entry.entity_type_enum = ?");

            objectArray[arrayPos] = entityType;
            arrayPos = arrayPos + 1;
        }

        if (searchParameters.isOfficeIdPassed()) {
            whereClause.append(whereClause.length() == 0 ? " where " : " and ");

            whereClause.append(" journal_entry.office_id = ?");
            objectArray[arrayPos] = searchParameters.getOfficeId();
            arrayPos = arrayPos + 1;
        }

        if (searchParameters.isCurrencyCodePassed()) {
            whereClause.append(whereClause.length() == 0 ? " where " : " and ");

            whereClause.append(" journal_entry.currency_code = ?");
            objectArray[arrayPos] = searchParameters.getCurrencyCode();
            arrayPos = arrayPos + 1;
        }

        if (glAccountId != null && glAccountId != 0) {
            whereClause.append(whereClause.length() == 0 ? " where " : " and ");

            whereClause.append(" journal_entry.account_id = ?");
            objectArray[arrayPos] = glAccountId;
            arrayPos = arrayPos + 1;
        }

        if (fromDate != null || toDate != null) {
            whereClause.append(whereClause.length() == 0 ? " where " : " and ");

            if (fromDate != null && toDate != null) {
                whereClause.append(" journal_entry.transaction_date between ? and ? ");
                objectArray[arrayPos] = fromDate;
                arrayPos = arrayPos + 1;
                objectArray[arrayPos] = toDate;
                arrayPos = arrayPos + 1;
            } else if (fromDate != null) {
                whereClause.append(" journal_entry.transaction_date >= ? ");
                objectArray[arrayPos] = fromDate;
                arrayPos = arrayPos + 1;
            } else if (toDate != null) {
                whereClause.append(" journal_entry.transaction_date <= ? ");
                objectArray[arrayPos] = toDate;
                arrayPos = arrayPos + 1;
            }
        }

       if (Boolean.TRUE.equals(onlyManualEntries)) {
            whereClause.append(whereClause.length() == 0 ? " where " : " and ");

            whereClause.append(" journal_entry.manual_entry = " + sqlResolver.formatBoolValue(Boolean.TRUE));
        }

        if (searchParameters.isLoanIdPassed()) {
            whereClause.append(whereClause.length() == 0 ? " where " : " and ");

            whereClause.append(" journal_entry.loan_transaction_id  in (select id from m_loan_transaction where loan_id = ?)");
            objectArray[arrayPos] = searchParameters.getLoanId();
            arrayPos = arrayPos + 1;
        }
        if (searchParameters.isSavingsIdPassed()) {
            whereClause.append(whereClause.length() == 0 ? " where " : " and ");

            whereClause.append(" journal_entry.savings_transaction_id in (select id from m_savings_account_transaction where savings_account_id = ?)");
            objectArray[arrayPos] = searchParameters.getSavingsId();
            arrayPos = arrayPos + 1;
        }
        sqlBuilder.append(whereClause.toString());

        if (searchParameters.isOrderByRequested()) {
            sqlBuilder.append(" order by ").append(searchParameters.getOrderBy());
            this.columnValidator.validateSqlInjection(sqlBuilder.toString(), searchParameters.getOrderBy());

            if (searchParameters.isSortOrderProvided()) {
                sqlBuilder.append(' ').append(searchParameters.getSortOrder());
                this.columnValidator.validateSqlInjection(sqlBuilder.toString(), searchParameters.getOrderBy());
            }
        } else {
            sqlBuilder.append(" order by journal_entry.entry_date, journal_entry.id");
        }

        if (searchParameters.isLimited()) {
            sqlBuilder.append(" limit ").append(searchParameters.getLimit());
            if (searchParameters.isOffset()) {
                sqlBuilder.append(" offset ").append(searchParameters.getOffset());
            }
        }

        final Object[] sqlParams = Arrays.copyOf(objectArray, arrayPos);
        String sqlCountRows = "SELECT FOUND_ROWS()";
        Object[] countParams = null;
        if (!mySql) {
            sqlCountRows = "SELECT " + rm.countSchema() + whereClause;
            countParams = sqlParams;
        }

        return this.paginationHelper.fetchPage(this.jdbcTemplate, sqlCountRows, countParams, sqlBuilder.toString(), sqlParams, rm);
    }

    @Override
    public JournalEntryData retrieveGLJournalEntryById(final long glJournalEntryId,
            JournalEntryAssociationParametersData associationParametersData) {
        try {

            final GLJournalEntryMapper rm = new GLJournalEntryMapper(associationParametersData);
            final String sql = "select " + rm.schema() + " where journalEntry.id = ?";

            final JournalEntryData glJournalEntryData = this.jdbcTemplate.queryForObject(sql, rm, new Object[] { glJournalEntryId });

            return glJournalEntryData;
        } catch (final EmptyResultDataAccessException e) {
            throw new JournalEntriesNotFoundException(glJournalEntryId, e);
        }
    }

    @Override
    public OfficeOpeningBalancesData retrieveOfficeOpeningBalances(final Long officeId, String currencyCode) {

        final FinancialActivityAccount financialActivityAccountId = this.financialActivityAccountRepositoryWrapper
                .findByFinancialActivityTypeWithNotFoundDetection(300);
        final Long contraId = financialActivityAccountId.getGlAccount().getId();
        if (contraId == null) {
            throw new GeneralPlatformDomainRuleException(
                    "error.msg.financial.activity.mapping.opening.balance.contra.account.cannot.be.null",
                    "office-opening-balances-contra-account value can not be null", "office-opening-balances-contra-account");
        }

        final JournalEntryAssociationParametersData associationParametersData = new JournalEntryAssociationParametersData();
        final GLAccountData contraAccount = this.glAccountReadPlatformService.retrieveGLAccountById(contraId, associationParametersData);
        if (!GLAccountType.fromInt(contraAccount.getTypeId()).isEquityType()) {
            throw new GeneralPlatformDomainRuleException(
                    "error.msg.configuration.opening.balance.contra.account.value.is.invalid.account.type",
                    "Global configuration 'office-opening-balances-contra-account' value is not an equity type account", contraId);
        }

        final OfficeData officeData = this.officeReadPlatformService.retrieveOffice(officeId);
        final List<JournalEntryData> allOpeningTransactions = populateAllTransactionsFromGLAccounts(contraId);
        final String contraTransactionId = retrieveContraAccountTransactionId(officeId, contraId, currencyCode);

        List<JournalEntryData> existingOpeningBalanceTransactions = new ArrayList<>();
        if (StringUtils.isNotBlank(contraTransactionId)) {
            existingOpeningBalanceTransactions = retrieveOfficeBalanceTransactions(officeId, contraTransactionId, currencyCode);
        }
        final List<JournalEntryData> transactions = populateOpeningBalances(existingOpeningBalanceTransactions, allOpeningTransactions);
        final List<JournalEntryData> assetAccountOpeningBalances = new ArrayList<>();
        final List<JournalEntryData> liabityAccountOpeningBalances = new ArrayList<>();
        final List<JournalEntryData> incomeAccountOpeningBalances = new ArrayList<>();
        final List<JournalEntryData> equityAccountOpeningBalances = new ArrayList<>();
        final List<JournalEntryData> expenseAccountOpeningBalances = new ArrayList<>();

        for (final JournalEntryData journalEntryData : transactions) {
            final GLAccountType type = GLAccountType.fromInt(journalEntryData.getGlAccountType().getId().intValue());
            if (type.isAssetType()) {
                assetAccountOpeningBalances.add(journalEntryData);
            } else if (type.isLiabilityType()) {
                liabityAccountOpeningBalances.add(journalEntryData);
            } else if (type.isEquityType()) {
                equityAccountOpeningBalances.add(journalEntryData);
            } else if (type.isIncomeType()) {
                incomeAccountOpeningBalances.add(journalEntryData);
            } else if (type.isExpenseType()) {
                expenseAccountOpeningBalances.add(journalEntryData);
            }
        }

        final LocalDate transactionDate = DateUtils.getLocalDateOfTenant();

        final OfficeOpeningBalancesData officeOpeningBalancesData = OfficeOpeningBalancesData.createNew(officeId, officeData.name(),
                transactionDate, contraAccount, assetAccountOpeningBalances, liabityAccountOpeningBalances, incomeAccountOpeningBalances,
                equityAccountOpeningBalances, expenseAccountOpeningBalances);
        return officeOpeningBalancesData;
    }

    private List<JournalEntryData> populateOpeningBalances(final List<JournalEntryData> existingOpeningBalanceTransactions,
            final List<JournalEntryData> allOpeningTransactions) {
        final List<JournalEntryData> allOpeningBalnceTransactions = new ArrayList<>(allOpeningTransactions.size());
        for (final JournalEntryData newOpeningBalanceTransaction : allOpeningTransactions) {
            boolean isNewTransactionAddedToCollection = false;
            for (final JournalEntryData existingOpeningBalanceTransaction : existingOpeningBalanceTransactions) {
                if (newOpeningBalanceTransaction.getGlAccountId().equals(existingOpeningBalanceTransaction.getGlAccountId())) {
                    allOpeningBalnceTransactions.add(existingOpeningBalanceTransaction);
                    isNewTransactionAddedToCollection = true;
                    break;
                }
            }
            if (!isNewTransactionAddedToCollection) {
                allOpeningBalnceTransactions.add(newOpeningBalanceTransaction);
            }
        }
        return allOpeningBalnceTransactions;
    }

    private List<JournalEntryData> populateAllTransactionsFromGLAccounts(final Long contraId) {
        final List<GLAccountData> glAccounts = this.glAccountReadPlatformService.retrieveAllEnabledDetailGLAccounts();
        final List<JournalEntryData> openingBalanceTransactions = new ArrayList<>(glAccounts.size());

        for (final GLAccountData glAccountData : glAccounts) {
            if (!contraId.equals(glAccountData.getId())) {
                final JournalEntryData openingBalanceTransaction = JournalEntryData.fromGLAccountData(glAccountData);
                openingBalanceTransactions.add(openingBalanceTransaction);
            }
        }
        return openingBalanceTransactions;
    }

    private List<JournalEntryData> retrieveOfficeBalanceTransactions(final Long officeId, final String transactionId,
            final String currencyCode) {
        final Long contraId = null;
        return retrieveContraTransactions(officeId, contraId, transactionId, currencyCode).getPageItems();
    }

    private String retrieveContraAccountTransactionId(final Long officeId, final Long contraId, final String currencyCode) {
        final String transactionId = "";
        final Page<JournalEntryData> contraJournalEntries = retrieveContraTransactions(officeId, contraId, transactionId, currencyCode);
        if (!CollectionUtils.isEmpty(contraJournalEntries.getPageItems())) {
            final JournalEntryData contraTransaction = contraJournalEntries.getPageItems()
                    .get(contraJournalEntries.getPageItems().size() - 1);
            return contraTransaction.getTransactionId();
        }
        return transactionId;
    }

    private Page<JournalEntryData> retrieveContraTransactions(final Long officeId, final Long contraId, final String transactionId,
            final String currencyCode) {
        final Integer offset = 0;
        final Integer limit = null;
        final String orderBy = "journalEntry.id";
        final String sortOrder = "ASC";
        final Integer entityType = null;
        final Boolean onlyManualEntries = null;
        final Date fromDate = null;
        final Date toDate = null;
        final JournalEntryAssociationParametersData associationParametersData = null;
        final Long loanId = null;
        final Long savingsId = null;

        final SearchParameters searchParameters = SearchParameters.forJournalEntries(officeId, offset, limit, orderBy, sortOrder, loanId,
                savingsId, currencyCode);
        return retrieveAll(searchParameters, contraId, onlyManualEntries, fromDate, toDate, transactionId, entityType,
                associationParametersData);

    }

    @Override
    public Page<JournalEntryData> retrieveJournalEntriesByEntityId(String transactionId, Long entityId, Integer entityType) {
        JournalEntryAssociationParametersData associationParametersData = new JournalEntryAssociationParametersData(true, true);
        try {
            final GLJournalEntryMapper rm = new GLJournalEntryMapper(associationParametersData);
            boolean mySql = sqlResolver.getDialect().isMySql();
            final StringBuilder sqlBuilder = new StringBuilder("select ");
            if (mySql) {
                sqlBuilder.append("SQL_CALC_FOUND_ROWS ");
            }

            String where = " where journal_entry.transaction_id = ? and journal_entry.entity_id = ? and journal_entry.entity_type_enum = ?";
            sqlBuilder.append(rm.schema()).append(where);

            Object[] sqlParams = {transactionId, entityId, entityType} ;
            String sqlCountRows = "SELECT FOUND_ROWS()";
            Object[] countParams = null;
            if (!mySql) {
                sqlCountRows = "SELECT " + rm.countSchema() + where;
                countParams = sqlParams;
            }
            return this.paginationHelper.fetchPage(this.jdbcTemplate, sqlCountRows, countParams, sqlBuilder.toString(), sqlParams, rm);
        } catch (final EmptyResultDataAccessException e) {
            throw new JournalEntriesNotFoundException(entityId, e);
        }
    }
}
