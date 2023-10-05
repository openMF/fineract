package org.apache.fineract.accounting.journalentry;

import javax.annotation.processing.Generated;
import org.apache.fineract.accounting.glaccount.domain.GLAccount;
import org.apache.fineract.accounting.journalentry.data.JournalEntryData;
import org.apache.fineract.accounting.journalentry.domain.JournalEntry;
import org.apache.fineract.organisation.office.domain.Office;
import org.apache.fineract.portfolio.paymentdetail.domain.PaymentDetail;
import org.apache.fineract.portfolio.paymenttype.domain.PaymentType;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2023-10-05T21:50:03+0200",
    comments = "version: 1.5.5.Final, compiler: javac, environment: Java 17.0.8.1 (N/A)"
)
@Component
public class JournalEntryMapperImpl implements JournalEntryMapper {

    @Override
    public JournalEntryData map(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }

        JournalEntryData journalEntryData = new JournalEntryData();

        journalEntryData.setId( journalEntry.getId() );
        journalEntryData.setOfficeId( journalEntryOfficeId( journalEntry ) );
        journalEntryData.setOfficeName( journalEntryOfficeName( journalEntry ) );
        journalEntryData.setGlAccountId( journalEntryGlAccountId( journalEntry ) );
        journalEntryData.setGlAccountCode( journalEntryGlAccountGlCode( journalEntry ) );
        journalEntryData.setGlAccountName( journalEntryGlAccountName( journalEntry ) );
        journalEntryData.setGlAccountType( mapGlAccountType( mapGlAccountType( journalEntryGlAccountType( journalEntry ) ) ) );
        journalEntryData.setTransactionDate( journalEntry.getTransactionDate() );
        journalEntryData.setEntryType( mapJournalEntryType( mapJournalEntryType( journalEntry.getType() ) ) );
        journalEntryData.setAmount( journalEntry.getAmount() );
        journalEntryData.setEntityType( mapEntityType( mapEntityType( journalEntry.getEntityType() ) ) );
        journalEntryData.setEntityId( journalEntry.getEntityId() );
        journalEntryData.setSubmittedOnDate( journalEntry.getSubmittedOnDate() );
        journalEntryData.setTransactionId( journalEntry.getTransactionId() );
        journalEntryData.setCurrency( mapCurrency( journalEntry.getCurrencyCode() ) );
        journalEntryData.setManualEntry( journalEntry.isManualEntry() );
        journalEntryData.setReversed( journalEntry.isReversed() );
        journalEntryData.setReferenceNumber( journalEntry.getReferenceNumber() );
        journalEntryData.setPaymentTypeId( journalEntryPaymentDetailPaymentTypeId( journalEntry ) );
        journalEntryData.setAccountNumber( journalEntryPaymentDetailAccountNumber( journalEntry ) );
        journalEntryData.setCheckNumber( journalEntryPaymentDetailCheckNumber( journalEntry ) );
        journalEntryData.setRoutingCode( journalEntryPaymentDetailRoutingCode( journalEntry ) );
        journalEntryData.setReceiptNumber( journalEntryPaymentDetailReceiptNumber( journalEntry ) );
        journalEntryData.setBankNumber( journalEntryPaymentDetailBankNumber( journalEntry ) );
        journalEntryData.setCurrencyCode( journalEntry.getCurrencyCode() );

        return journalEntryData;
    }

    private Long journalEntryOfficeId(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        Office office = journalEntry.getOffice();
        if ( office == null ) {
            return null;
        }
        Long id = office.getId();
        if ( id == null ) {
            return null;
        }
        return id;
    }

    private String journalEntryOfficeName(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        Office office = journalEntry.getOffice();
        if ( office == null ) {
            return null;
        }
        String name = office.getName();
        if ( name == null ) {
            return null;
        }
        return name;
    }

    private Long journalEntryGlAccountId(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        GLAccount glAccount = journalEntry.getGlAccount();
        if ( glAccount == null ) {
            return null;
        }
        Long id = glAccount.getId();
        if ( id == null ) {
            return null;
        }
        return id;
    }

    private String journalEntryGlAccountGlCode(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        GLAccount glAccount = journalEntry.getGlAccount();
        if ( glAccount == null ) {
            return null;
        }
        String glCode = glAccount.getGlCode();
        if ( glCode == null ) {
            return null;
        }
        return glCode;
    }

    private String journalEntryGlAccountName(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        GLAccount glAccount = journalEntry.getGlAccount();
        if ( glAccount == null ) {
            return null;
        }
        String name = glAccount.getName();
        if ( name == null ) {
            return null;
        }
        return name;
    }

    private Integer journalEntryGlAccountType(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        GLAccount glAccount = journalEntry.getGlAccount();
        if ( glAccount == null ) {
            return null;
        }
        Integer type = glAccount.getType();
        if ( type == null ) {
            return null;
        }
        return type;
    }

    private Long journalEntryPaymentDetailPaymentTypeId(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        PaymentDetail paymentDetail = journalEntry.getPaymentDetail();
        if ( paymentDetail == null ) {
            return null;
        }
        PaymentType paymentType = paymentDetail.getPaymentType();
        if ( paymentType == null ) {
            return null;
        }
        Long id = paymentType.getId();
        if ( id == null ) {
            return null;
        }
        return id;
    }

    private String journalEntryPaymentDetailAccountNumber(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        PaymentDetail paymentDetail = journalEntry.getPaymentDetail();
        if ( paymentDetail == null ) {
            return null;
        }
        String accountNumber = paymentDetail.getAccountNumber();
        if ( accountNumber == null ) {
            return null;
        }
        return accountNumber;
    }

    private String journalEntryPaymentDetailCheckNumber(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        PaymentDetail paymentDetail = journalEntry.getPaymentDetail();
        if ( paymentDetail == null ) {
            return null;
        }
        String checkNumber = paymentDetail.getCheckNumber();
        if ( checkNumber == null ) {
            return null;
        }
        return checkNumber;
    }

    private String journalEntryPaymentDetailRoutingCode(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        PaymentDetail paymentDetail = journalEntry.getPaymentDetail();
        if ( paymentDetail == null ) {
            return null;
        }
        String routingCode = paymentDetail.getRoutingCode();
        if ( routingCode == null ) {
            return null;
        }
        return routingCode;
    }

    private String journalEntryPaymentDetailReceiptNumber(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        PaymentDetail paymentDetail = journalEntry.getPaymentDetail();
        if ( paymentDetail == null ) {
            return null;
        }
        String receiptNumber = paymentDetail.getReceiptNumber();
        if ( receiptNumber == null ) {
            return null;
        }
        return receiptNumber;
    }

    private String journalEntryPaymentDetailBankNumber(JournalEntry journalEntry) {
        if ( journalEntry == null ) {
            return null;
        }
        PaymentDetail paymentDetail = journalEntry.getPaymentDetail();
        if ( paymentDetail == null ) {
            return null;
        }
        String bankNumber = paymentDetail.getBankNumber();
        if ( bankNumber == null ) {
            return null;
        }
        return bankNumber;
    }
}
