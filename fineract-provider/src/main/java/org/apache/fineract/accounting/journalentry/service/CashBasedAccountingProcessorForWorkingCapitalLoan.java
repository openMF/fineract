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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.accounting.common.AccountingConstants.CashAccountsForLoan;
import org.apache.fineract.accounting.glaccount.domain.GLAccount;
import org.apache.fineract.accounting.journalentry.domain.JournalEntry;
import org.apache.fineract.accounting.journalentry.domain.JournalEntryRepository;
import org.apache.fineract.accounting.journalentry.domain.JournalEntryType;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.core.service.MathUtil;
import org.apache.fineract.organisation.office.domain.Office;
import org.apache.fineract.portfolio.PortfolioProductType;
import org.apache.fineract.portfolio.workingcapitalloan.accounting.WorkingCapitalLoanAccountingProcessor;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoan;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanTransaction;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanTransactionAllocation;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CashBasedAccountingProcessorForWorkingCapitalLoan implements WorkingCapitalLoanAccountingProcessor {

    private static final int WORKING_CAPITAL_LOAN_ENTITY_TYPE = PortfolioProductType.WORKING_CAPITAL_LOAN.getValue();

    private final AccountingProcessorHelper helper;
    private final JournalEntryRepository journalEntryRepository;

    @Override
    public void postJournalEntriesForRepayment(final WorkingCapitalLoan loan, final WorkingCapitalLoanTransaction txn,
            final WorkingCapitalLoanTransactionAllocation allocation, final boolean isChargedOff) {
        final Office office = loan.getClient().getOffice();
        final Long productId = loan.getLoanProduct().getId();
        final String currencyCode = loan.getLoanProductRelatedDetails().getCurrency().getCode();
        final LocalDate transactionDate = txn.getTransactionDate();
        final Long paymentTypeId = extractPaymentTypeId(txn);

        helper.checkForBranchClosures(helper.getLatestClosureByBranch(office.getId()), transactionDate);

        final BigDecimal principalPortion = MathUtil.nullToZero(allocation.getPrincipalPortion());
        final BigDecimal feesPortion = MathUtil.nullToZero(allocation.getFeeChargesPortion());
        final BigDecimal penaltiesPortion = MathUtil.nullToZero(allocation.getPenaltyChargesPortion());
        final BigDecimal overpaymentPortion = txn.getTransactionAmount().subtract(principalPortion).subtract(feesPortion)
                .subtract(penaltiesPortion).max(BigDecimal.ZERO);

        if (isChargedOff) {
            postChargedOffRepaymentEntries(office, productId, currencyCode, transactionDate, paymentTypeId, txn, principalPortion,
                    feesPortion, penaltiesPortion, overpaymentPortion);
        } else {
            postRegularRepaymentEntries(office, productId, currencyCode, transactionDate, paymentTypeId, txn, principalPortion, feesPortion,
                    penaltiesPortion, overpaymentPortion);
        }
    }

    @Override
    public void postReversalJournalEntries(final WorkingCapitalLoan loan, final WorkingCapitalLoanTransaction txn) {
        final Office office = loan.getClient().getOffice();
        final LocalDate transactionDate = txn.getReversedOnDate() != null ? txn.getReversedOnDate() : DateUtils.getBusinessLocalDate();

        helper.checkForBranchClosures(helper.getLatestClosureByBranch(office.getId()), transactionDate);

        final String transactionId = AccountingProcessorHelper.WORKING_CAPITAL_LOAN_TRANSACTION_IDENTIFIER + txn.getId();
        final List<JournalEntry> existingEntries = journalEntryRepository.findJournalEntries(transactionId,
                WORKING_CAPITAL_LOAN_ENTITY_TYPE);

        for (final JournalEntry journalEntry : existingEntries) {
            final JournalEntryType reversalType = journalEntry.isDebitEntry() ? JournalEntryType.CREDIT : JournalEntryType.DEBIT;
            final JournalEntry reversalEntry = JournalEntry.createNew(journalEntry.getOffice(), journalEntry.getPaymentDetail(),
                    journalEntry.getGlAccount(), journalEntry.getCurrencyCode(), transactionId, Boolean.FALSE, transactionDate,
                    reversalType, journalEntry.getAmount(), journalEntry.getDescription(), journalEntry.getEntityType(),
                    journalEntry.getEntityId(), journalEntry.getReferenceNumber(), journalEntry.getLoanTransactionId(),
                    journalEntry.getSavingsTransactionId(), journalEntry.getClientTransactionId(), journalEntry.getShareTransactionId());
            helper.persistJournalEntry(reversalEntry);

            journalEntry.setReversed(true);
            journalEntry.setReversalJournalEntry(reversalEntry);
            helper.persistJournalEntry(journalEntry);
        }
    }

    private void postRegularRepaymentEntries(final Office office, final Long productId, final String currencyCode,
            final LocalDate transactionDate, final Long paymentTypeId, final WorkingCapitalLoanTransaction txn,
            final BigDecimal principalPortion, final BigDecimal feesPortion, final BigDecimal penaltiesPortion,
            final BigDecimal overpaymentPortion) {
        postRepaymentCreditEntries(office, productId, currencyCode, transactionDate, txn, principalPortion,
                CashAccountsForLoan.LOAN_PORTFOLIO, feesPortion, CashAccountsForLoan.FEES_RECEIVABLE, penaltiesPortion,
                CashAccountsForLoan.PENALTIES_RECEIVABLE, overpaymentPortion);
        postFundSourceDebit(office, productId, currencyCode, transactionDate, paymentTypeId, txn);
    }

    private void postChargedOffRepaymentEntries(final Office office, final Long productId, final String currencyCode,
            final LocalDate transactionDate, final Long paymentTypeId, final WorkingCapitalLoanTransaction txn,
            final BigDecimal principalPortion, final BigDecimal feesPortion, final BigDecimal penaltiesPortion,
            final BigDecimal overpaymentPortion) {
        postRepaymentCreditEntries(office, productId, currencyCode, transactionDate, txn, principalPortion,
                CashAccountsForLoan.INCOME_FROM_RECOVERY, feesPortion, CashAccountsForLoan.INCOME_FROM_RECOVERY, penaltiesPortion,
                CashAccountsForLoan.INCOME_FROM_RECOVERY, overpaymentPortion);
        postFundSourceDebit(office, productId, currencyCode, transactionDate, paymentTypeId, txn);
    }

    private void postRepaymentCreditEntries(final Office office, final Long productId, final String currencyCode,
            final LocalDate transactionDate, final WorkingCapitalLoanTransaction txn, final BigDecimal principalPortion,
            final CashAccountsForLoan principalAccountType, final BigDecimal feesPortion, final CashAccountsForLoan feesAccountType,
            final BigDecimal penaltiesPortion, final CashAccountsForLoan penaltiesAccountType, final BigDecimal overpaymentPortion) {
        final Long loanId = txn.getWcLoan().getId();
        final Long txnId = txn.getId();

        if (MathUtil.isGreaterThanZero(principalPortion)) {
            final GLAccount account = helper.getLinkedGLAccountForWorkingCapitalLoanProduct(productId, principalAccountType.getValue(),
                    null);
            helper.createCreditJournalEntryForWorkingCapitalLoan(office, currencyCode, account, loanId, txnId, transactionDate,
                    principalPortion, null);
        }

        if (MathUtil.isGreaterThanZero(feesPortion)) {
            final GLAccount account = helper.getLinkedGLAccountForWorkingCapitalLoanProduct(productId, feesAccountType.getValue(), null);
            helper.createCreditJournalEntryForWorkingCapitalLoan(office, currencyCode, account, loanId, txnId, transactionDate, feesPortion,
                    null);
        }

        if (MathUtil.isGreaterThanZero(penaltiesPortion)) {
            final GLAccount account = helper.getLinkedGLAccountForWorkingCapitalLoanProduct(productId, penaltiesAccountType.getValue(),
                    null);
            helper.createCreditJournalEntryForWorkingCapitalLoan(office, currencyCode, account, loanId, txnId, transactionDate,
                    penaltiesPortion, null);
        }

        if (MathUtil.isGreaterThanZero(overpaymentPortion)) {
            final GLAccount account = helper.getLinkedGLAccountForWorkingCapitalLoanProduct(productId,
                    CashAccountsForLoan.OVERPAYMENT.getValue(), null);
            helper.createCreditJournalEntryForWorkingCapitalLoan(office, currencyCode, account, loanId, txnId, transactionDate,
                    overpaymentPortion, null);
        }
    }

    private void postFundSourceDebit(final Office office, final Long productId, final String currencyCode, final LocalDate transactionDate,
            final Long paymentTypeId, final WorkingCapitalLoanTransaction txn) {
        final BigDecimal totalAmount = txn.getTransactionAmount();
        if (MathUtil.isGreaterThanZero(totalAmount)) {
            final GLAccount account = helper.getLinkedGLAccountForWorkingCapitalLoanProduct(productId,
                    CashAccountsForLoan.FUND_SOURCE.getValue(), paymentTypeId);
            helper.createDebitJournalEntryForWorkingCapitalLoan(office, currencyCode, account, txn.getWcLoan().getId(), txn.getId(),
                    transactionDate, totalAmount, txn.getPaymentDetail());
        }
    }

    private Long extractPaymentTypeId(final WorkingCapitalLoanTransaction txn) {
        if (txn.getPaymentDetail() != null && txn.getPaymentDetail().getPaymentType() != null) {
            return txn.getPaymentDetail().getPaymentType().getId();
        }
        return null;
    }
}
