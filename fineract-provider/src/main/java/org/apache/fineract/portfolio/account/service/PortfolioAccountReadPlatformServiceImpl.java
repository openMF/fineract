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
package org.apache.fineract.portfolio.account.service;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.fineract.infrastructure.core.boot.db.DataSourceSqlResolver;
import org.apache.fineract.infrastructure.core.domain.JdbcSupport;
import org.apache.fineract.infrastructure.core.service.RoutingDataSource;
import org.apache.fineract.organisation.monetary.data.CurrencyData;
import org.apache.fineract.portfolio.account.PortfolioAccountType;
import org.apache.fineract.portfolio.account.data.PortfolioAccountDTO;
import org.apache.fineract.portfolio.account.data.PortfolioAccountData;
import org.apache.fineract.portfolio.account.exception.AccountTransferNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class PortfolioAccountReadPlatformServiceImpl implements PortfolioAccountReadPlatformService {

    private final JdbcTemplate jdbcTemplate;
    private final DataSourceSqlResolver sqlResolver;

    // mapper
    private final PortfolioSavingsAccountMapper savingsAccountMapper;
    private final PortfolioLoanAccountMapper loanAccountMapper;
    private final PortfolioLoanAccountRefundByTransferMapper accountRefundByTransferMapper;

    @Autowired
    public PortfolioAccountReadPlatformServiceImpl(final RoutingDataSource dataSource, DataSourceSqlResolver sqlResolver) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.sqlResolver = sqlResolver;
        this.savingsAccountMapper = new PortfolioSavingsAccountMapper();
        this.loanAccountMapper = new PortfolioLoanAccountMapper();
        this.accountRefundByTransferMapper = new PortfolioLoanAccountRefundByTransferMapper();
    }

    @Override
    public PortfolioAccountData retrieveOne(final Long accountId, final Integer accountTypeId) {
        return retrieveOne(accountId, accountTypeId, null);
    }

    @Override
    public PortfolioAccountData retrieveOne(final Long accountId, final Integer accountTypeId, final String currencyCode) {

        Object[] sqlParams = new Object[] { accountId };
        PortfolioAccountData accountData = null;
        try {
            String sql = null;
            final PortfolioAccountType accountType = PortfolioAccountType.fromInt(accountTypeId);
            switch (accountType) {
                case INVALID:
                break;
                case LOAN:

                    sql = "select " + this.loanAccountMapper.schema() + " where la.id = ?";
                    if (currencyCode != null) {
                        sql += " and la.currency_code = ?";
                        sqlParams = new Object[] { accountId, currencyCode };
                    }

                    accountData = this.jdbcTemplate.queryForObject(sql, this.loanAccountMapper, sqlParams);
                break;
                case SAVINGS:
                    sql = "select " + this.savingsAccountMapper.schema() + " where sa.id = ?";
                    if (currencyCode != null) {
                        sql += " and sa.currency_code = ?";
                        sqlParams = new Object[] { accountId, currencyCode };
                    }

                    accountData = this.jdbcTemplate.queryForObject(sql, this.savingsAccountMapper, sqlParams);
                break;
            }
        } catch (final EmptyResultDataAccessException e) {
            throw new AccountTransferNotFoundException(accountId, e);
        }

        return accountData;
    }

    @Override
    public Collection<PortfolioAccountData> retrieveAllForLookup(final PortfolioAccountDTO portfolioAccountDTO) {

        final List<Object> sqlParams = new ArrayList<>();
        // sqlParams.add(portfolioAccountDTO.getClientId());
        Collection<PortfolioAccountData> accounts = null;
        String sql = null;
        String defaultAccountStatus = "300";
        if (portfolioAccountDTO.getAccountStatus() != null) {
            for (final long status : portfolioAccountDTO.getAccountStatus()) {
                defaultAccountStatus += ", " + status;
            }
            defaultAccountStatus = defaultAccountStatus.substring(defaultAccountStatus.indexOf(",") + 1);
        }
        final PortfolioAccountType accountType = PortfolioAccountType.fromInt(portfolioAccountDTO.getAccountTypeId());
        switch (accountType) {
            case INVALID:
            break;
            case LOAN:
                sql = "select " + this.loanAccountMapper.schema() + " where ";
                if (portfolioAccountDTO.getClientId() != null) {
                    sql += " la.client_id = ? and la.loan_status_id in (" + defaultAccountStatus.toString() + ") ";
                    sqlParams.add(portfolioAccountDTO.getClientId());
                } else {
                    sql += " la.loan_status_id in (" + defaultAccountStatus.toString() + ") ";
                }
                if (portfolioAccountDTO.getCurrencyCode() != null) {
                    sql += " and la.currency_code = ?";
                    sqlParams.add(portfolioAccountDTO.getCurrencyCode());
                }

                accounts = this.jdbcTemplate.query(sql, this.loanAccountMapper, sqlParams.toArray());
            break;
            case SAVINGS:
                sql = "select " + this.savingsAccountMapper.schema() + " where ";
                if (portfolioAccountDTO.getClientId() != null) {
                    sql += " sa.client_id = ? and sa.status_enum in (" + defaultAccountStatus.toString() + ") ";
                    sqlParams.add(portfolioAccountDTO.getClientId());
                } else {
                    sql += " sa.status_enum in (" + defaultAccountStatus.toString() + ") ";
                }
                if (portfolioAccountDTO.getCurrencyCode() != null) {
                    sql += " and sa.currency_code = ?";
                    sqlParams.add(portfolioAccountDTO.getCurrencyCode());
                }

                if (portfolioAccountDTO.getDepositType() != null) {
                    sql += " and sa.deposit_type_enum = ?";
                    sqlParams.add(portfolioAccountDTO.getDepositType());
                }

                if (portfolioAccountDTO.isExcludeOverDraftAccounts()) {
                    sql += " and sa.allow_overdraft = " + sqlResolver.formatBoolValue(false);
                }

                if (portfolioAccountDTO.getClientId() == null && portfolioAccountDTO.getGroupId() != null) {
                    sql += " and sa.group_id = ? ";
                    sqlParams.add(portfolioAccountDTO.getGroupId());
                }

                accounts = this.jdbcTemplate.query(sql, this.savingsAccountMapper, sqlParams.toArray());
            break;
        }

        return accounts;
    }

    private static final class PortfolioSavingsAccountMapper implements RowMapper<PortfolioAccountData> {

        private final String schemaSql;

        public PortfolioSavingsAccountMapper() {

            final StringBuilder sqlBuilder = new StringBuilder(400);
            sqlBuilder.append("sa.id as id, sa.account_no as account_no, sa.external_id as external_id, ");
            sqlBuilder.append("c.id as client_id, c.display_name as client_name, ");
            sqlBuilder.append("g.id as group_id, g.display_name as group_name, ");
            sqlBuilder.append("sp.id as product_id, sp.name as product_name, ");
            sqlBuilder.append("s.id as field_officer_id, s.display_name as field_officer_name, ");
            sqlBuilder.append("sa.currency_code as currency_code, sa.currency_digits as currency_digits,");
            sqlBuilder.append("sa.currency_multiplesof as in_multiples_of, ");
            sqlBuilder.append("curr.name as currency_name, curr.internationalized_name_code as currency_name_code, ");
            sqlBuilder.append("curr.display_symbol as currency_display_symbol ");
            sqlBuilder.append("from m_savings_account sa ");
            sqlBuilder.append("join m_savings_product sp ON sa.product_id = sp.id ");
            sqlBuilder.append("join m_currency curr on curr.code = sa.currency_code ");
            sqlBuilder.append("left join m_client c ON c.id = sa.client_id ");
            sqlBuilder.append("left join m_group g ON g.id = sa.group_id ");
            sqlBuilder.append("left join m_staff s ON s.id = sa.field_officer_id ");

            this.schemaSql = sqlBuilder.toString();
        }

        public String schema() {
            return this.schemaSql;
        }

        @Override
        public PortfolioAccountData mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {

            final Long id = rs.getLong("id");
            final String accountNo = rs.getString("account_no");
            final String externalId = rs.getString("external_id");

            final Long groupId = JdbcSupport.getLong(rs, "group_id");
            final String groupName = rs.getString("group_name");
            final Long clientId = JdbcSupport.getLong(rs, "client_id");
            final String clientName = rs.getString("client_name");

            final Long productId = rs.getLong("product_id");
            final String productName = rs.getString("product_name");

            final Long fieldOfficerId = rs.getLong("field_officer_id");
            final String fieldOfficerName = rs.getString("field_officer_name");

            final String currencyCode = rs.getString("currency_code");
            final String currencyName = rs.getString("currency_name");
            final String currencyNameCode = rs.getString("currency_name_code");
            final String currencyDisplaySymbol = rs.getString("currency_display_symbol");
            final Integer currencyDigits = JdbcSupport.getInteger(rs, "currency_digits");
            final Integer inMulitplesOf = JdbcSupport.getInteger(rs, "in_multiples_of");
            final CurrencyData currency = new CurrencyData(currencyCode, currencyName, currencyDigits, inMulitplesOf,
                    currencyDisplaySymbol, currencyNameCode);

            return new PortfolioAccountData(id, accountNo, externalId, groupId, groupName, clientId, clientName, productId, productName,
                    fieldOfficerId, fieldOfficerName, currency);
        }
    }

    private static final class PortfolioLoanAccountMapper implements RowMapper<PortfolioAccountData> {

        private final String schemaSql;

        public PortfolioLoanAccountMapper() {

            final StringBuilder sqlBuilder = new StringBuilder(400);
            sqlBuilder.append("la.id as id, la.account_no as account_no, la.external_id as external_id, ");
            sqlBuilder.append("c.id as client_id, c.display_name as client_name, ");
            sqlBuilder.append("g.id as group_id, g.display_name as group_name, ");
            sqlBuilder.append("lp.id as product_id, lp.name as product_name, ");
            sqlBuilder.append("s.id as field_officer_id, s.display_name as field_officer_name, ");
            sqlBuilder.append("la.currency_code as currency_code, la.currency_digits as currency_digits,");
            sqlBuilder.append("la.currency_multiplesof as in_multiples_of, ");
            sqlBuilder.append("la.total_overpaid_derived as total_overpaid, ");
            sqlBuilder.append("curr.name as currency_name, curr.internationalized_name_code as currency_name_code, ");
            sqlBuilder.append("curr.display_symbol as currency_display_symbol ");
            sqlBuilder.append("from m_loan la ");
            sqlBuilder.append("join m_product_loan lp ON la.product_id = lp.id ");
            sqlBuilder.append("join m_currency curr on curr.code = la.currency_code ");
            sqlBuilder.append("left join m_client c ON c.id = la.client_id ");
            sqlBuilder.append("left join m_group g ON g.id = la.group_id ");
            sqlBuilder.append("left join m_staff s ON s.id = la.loan_officer_id ");

            this.schemaSql = sqlBuilder.toString();
        }

        public String schema() {
            return this.schemaSql;
        }

        @Override
        public PortfolioAccountData mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {

            final Long id = rs.getLong("id");
            final String accountNo = rs.getString("account_no");
            final String externalId = rs.getString("external_id");

            final Long groupId = JdbcSupport.getLong(rs, "group_id");
            final String groupName = rs.getString("group_name");
            final Long clientId = JdbcSupport.getLong(rs, "client_id");
            final String clientName = rs.getString("client_name");

            final Long productId = rs.getLong("product_id");
            final String productName = rs.getString("product_name");

            final Long fieldOfficerId = rs.getLong("field_officer_id");
            final String fieldOfficerName = rs.getString("field_officer_name");

            final String currencyCode = rs.getString("currency_code");
            final String currencyName = rs.getString("currency_name");
            final String currencyNameCode = rs.getString("currency_name_code");
            final String currencyDisplaySymbol = rs.getString("currency_display_symbol");
            final Integer currencyDigits = JdbcSupport.getInteger(rs, "currency_digits");
            final Integer inMulitplesOf = JdbcSupport.getInteger(rs, "in_multiples_of");
            final BigDecimal amtForTransfer = JdbcSupport.getBigDecimalDefaultToNullIfZero(rs, "total_overpaid");
            final CurrencyData currency = new CurrencyData(currencyCode, currencyName, currencyDigits, inMulitplesOf,
                    currencyDisplaySymbol, currencyNameCode);

            return new PortfolioAccountData(id, accountNo, externalId, groupId, groupName, clientId, clientName, productId, productName,
                    fieldOfficerId, fieldOfficerName, currency, amtForTransfer);
        }
    }

    private final class PortfolioLoanAccountRefundByTransferMapper implements RowMapper<PortfolioAccountData> {

        private final String schemaSql;

        public PortfolioLoanAccountRefundByTransferMapper() {
            final StringBuilder amountQueryString = new StringBuilder(400);
            amountQueryString.append("(select (SUM(COALESCE(mr.principal_completed_derived, 0)) +");
            amountQueryString.append("SUM(COALESCE(mr.interest_completed_derived, 0)) + ");
             amountQueryString.append("SUM(COALESCE(mr.fee_charges_completed_derived, 0)) + ");
             amountQueryString.append(" SUM(COALESCE(mr.penalty_charges_completed_derived, 0))) as total_in_advance_derived");
            amountQueryString.append(" from m_loan ml INNER JOIN m_loan_repayment_schedule mr on mr.loan_id = ml.id");
            amountQueryString.append(" where ml.id=? and ml.loan_status_id = 300");
             amountQueryString.append("  and  mr.duedate >= ").append(sqlResolver.formatDateCurrent()).append(" group by ml.id having");
             amountQueryString.append(" (SUM(COALESCE(mr.principal_completed_derived, 0)) + ");
             amountQueryString.append(" SUM(COALESCE(mr.interest_completed_derived, 0)) + ");
             amountQueryString.append("SUM(COALESCE(mr.fee_charges_completed_derived, 0)) + ");
             amountQueryString.append("SUM(COALESCE(mr.penalty_charges_completed_derived, 0))) > 0) as total_overpaid ");

            final StringBuilder sqlBuilder = new StringBuilder(400);
            sqlBuilder.append("la.id as id, la.account_no as account_no, la.external_id as external_id, ");
            sqlBuilder.append("c.id as client_id, c.display_name as client_name, ");
            sqlBuilder.append("g.id as group_id, g.display_name as group_name, ");
            sqlBuilder.append("lp.id as product_id, lp.name as product_name, ");
            sqlBuilder.append("s.id as field_officer_id, s.display_name as field_officer_name, ");
            sqlBuilder.append("la.currency_code as currency_code, la.currency_digits as currency_digits,");
            sqlBuilder.append("la.currency_multiplesof as in_multiples_of, ");
            sqlBuilder.append(amountQueryString.toString());
            sqlBuilder.append(", ");
            sqlBuilder.append("curr.name as currency_name, curr.internationalized_name_code as currency_name_code, ");
            sqlBuilder.append("curr.display_symbol as currency_display_symbol ");
            sqlBuilder.append("from m_loan la ");
            sqlBuilder.append("join m_product_loan lp ON la.product_id = lp.id ");
            sqlBuilder.append("join m_currency curr on curr.code = la.currency_code ");
            sqlBuilder.append("left join m_client c ON c.id = la.client_id ");
            sqlBuilder.append("left join m_group g ON g.id = la.group_id ");
            sqlBuilder.append("left join m_staff s ON s.id = la.loan_officer_id ");

            this.schemaSql = sqlBuilder.toString();
        }

        public String schema() {
            return this.schemaSql;
        }

        @Override
        public PortfolioAccountData mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {

            final Long id = rs.getLong("id");
            final String accountNo = rs.getString("account_no");
            final String externalId = rs.getString("external_id");

            final Long groupId = JdbcSupport.getLong(rs, "group_id");
            final String groupName = rs.getString("group_name");
            final Long clientId = JdbcSupport.getLong(rs, "client_id");
            final String clientName = rs.getString("client_name");

            final Long productId = rs.getLong("product_id");
            final String productName = rs.getString("product_name");

            final Long fieldOfficerId = rs.getLong("field_officer_id");
            final String fieldOfficerName = rs.getString("field_officer_name");

            final String currencyCode = rs.getString("currency_code");
            final String currencyName = rs.getString("currency_name");
            final String currencyNameCode = rs.getString("currency_name_code");
            final String currencyDisplaySymbol = rs.getString("currency_display_symbol");
            final Integer currencyDigits = JdbcSupport.getInteger(rs, "currency_digits");
            final Integer inMulitplesOf = JdbcSupport.getInteger(rs, "in_multiples_of");
            final BigDecimal amtForTransfer = JdbcSupport.getBigDecimalDefaultToNullIfZero(rs, "total_overpaid");
            final CurrencyData currency = new CurrencyData(currencyCode, currencyName, currencyDigits, inMulitplesOf,
                    currencyDisplaySymbol, currencyNameCode);

            return new PortfolioAccountData(id, accountNo, externalId, groupId, groupName, clientId, clientName, productId, productName,
                    fieldOfficerId, fieldOfficerName, currency, amtForTransfer);
        }
    }

    @Override
    public PortfolioAccountData retrieveOneByPaidInAdvance(Long accountId, Integer accountTypeId) {
        // TODO Auto-generated method stub
        Object[] sqlParams = new Object[] { accountId, accountId };
        PortfolioAccountData accountData = null;
        // String currencyCode = null;
        try {
            String sql = null;
            // final PortfolioAccountType accountType =
            // PortfolioAccountType.fromInt(accountTypeId);

            sql = "select " + this.accountRefundByTransferMapper.schema() + " where la.id = ?";
            /*
             * if (currencyCode != null) { sql += " and la.currency_code = ?"; sqlParams = new Object[] {accountId ,
             * accountId,currencyCode }; }
             */

            accountData = this.jdbcTemplate.queryForObject(sql, this.accountRefundByTransferMapper, sqlParams);

        } catch (final EmptyResultDataAccessException e) {
            throw new AccountTransferNotFoundException(accountId, e);
        }

        return accountData;
    }
}
