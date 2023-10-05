package org.apache.fineract.investor.service;

import javax.annotation.processing.Generated;
import org.apache.fineract.infrastructure.event.external.service.serialization.mapper.support.ExternalIdMapper;
import org.apache.fineract.investor.data.ExternalTransferData;
import org.apache.fineract.investor.data.ExternalTransferDataDetails;
import org.apache.fineract.investor.data.ExternalTransferLoanData;
import org.apache.fineract.investor.data.ExternalTransferOwnerData;
import org.apache.fineract.investor.domain.ExternalAssetOwner;
import org.apache.fineract.investor.domain.ExternalAssetOwnerTransfer;
import org.apache.fineract.investor.domain.ExternalAssetOwnerTransferDetails;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2023-10-05T21:50:16+0200",
    comments = "version: 1.5.5.Final, compiler: javac, environment: Java 17.0.8.1 (N/A)"
)
@Component
public class ExternalAssetOwnersTransferMapperImpl implements ExternalAssetOwnersTransferMapper {

    private final ExternalIdMapper externalIdMapper;

    @Autowired
    public ExternalAssetOwnersTransferMapperImpl(ExternalIdMapper externalIdMapper) {

        this.externalIdMapper = externalIdMapper;
    }

    @Override
    public ExternalTransferData mapTransfer(ExternalAssetOwnerTransfer source) {
        if ( source == null ) {
            return null;
        }

        ExternalTransferData externalTransferData = new ExternalTransferData();

        externalTransferData.setLoan( externalAssetOwnerTransferToExternalTransferLoanData( source ) );
        externalTransferData.setTransferId( source.getId() );
        externalTransferData.setOwner( mapOwner( source.getOwner() ) );
        externalTransferData.setTransferExternalId( externalIdMapper.mapExternalId( source.getExternalId() ) );
        externalTransferData.setEffectiveFrom( source.getEffectiveDateFrom() );
        externalTransferData.setEffectiveTo( source.getEffectiveDateTo() );
        externalTransferData.setPurchasePriceRatio( source.getPurchasePriceRatio() );
        externalTransferData.setSettlementDate( source.getSettlementDate() );
        externalTransferData.setStatus( source.getStatus() );
        externalTransferData.setDetails( mapDetails( source.getExternalAssetOwnerTransferDetails() ) );
        externalTransferData.setSubStatus( source.getSubStatus() );

        return externalTransferData;
    }

    @Override
    public ExternalTransferOwnerData mapOwner(ExternalAssetOwner source) {
        if ( source == null ) {
            return null;
        }

        String externalId = null;

        externalId = externalIdMapper.mapExternalId( source.getExternalId() );

        ExternalTransferOwnerData externalTransferOwnerData = new ExternalTransferOwnerData( externalId );

        return externalTransferOwnerData;
    }

    @Override
    public ExternalTransferDataDetails mapDetails(ExternalAssetOwnerTransferDetails details) {
        if ( details == null ) {
            return null;
        }

        ExternalTransferDataDetails externalTransferDataDetails = new ExternalTransferDataDetails();

        externalTransferDataDetails.setDetailsId( details.getId() );
        externalTransferDataDetails.setTotalOutstanding( details.getTotalOutstanding() );
        externalTransferDataDetails.setTotalPrincipalOutstanding( details.getTotalPrincipalOutstanding() );
        externalTransferDataDetails.setTotalInterestOutstanding( details.getTotalInterestOutstanding() );
        externalTransferDataDetails.setTotalFeeChargesOutstanding( details.getTotalFeeChargesOutstanding() );
        externalTransferDataDetails.setTotalPenaltyChargesOutstanding( details.getTotalPenaltyChargesOutstanding() );
        externalTransferDataDetails.setTotalOverpaid( details.getTotalOverpaid() );

        return externalTransferDataDetails;
    }

    protected ExternalTransferLoanData externalAssetOwnerTransferToExternalTransferLoanData(ExternalAssetOwnerTransfer externalAssetOwnerTransfer) {
        if ( externalAssetOwnerTransfer == null ) {
            return null;
        }

        Long loanId = null;
        String externalId = null;

        loanId = externalAssetOwnerTransfer.getLoanId();
        externalId = externalIdMapper.mapExternalId( externalAssetOwnerTransfer.getExternalLoanId() );

        ExternalTransferLoanData externalTransferLoanData = new ExternalTransferLoanData( loanId, externalId );

        return externalTransferLoanData;
    }
}
