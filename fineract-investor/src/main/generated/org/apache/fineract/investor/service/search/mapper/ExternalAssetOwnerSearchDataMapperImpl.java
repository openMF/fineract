package org.apache.fineract.investor.service.search.mapper;

import javax.annotation.processing.Generated;
import org.apache.fineract.investor.data.ExternalTransferData;
import org.apache.fineract.investor.domain.search.SearchedExternalAssetOwner;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2023-10-05T21:50:16+0200",
    comments = "version: 1.5.5.Final, compiler: javac, environment: Java 17.0.8.1 (N/A)"
)
@Component
public class ExternalAssetOwnerSearchDataMapperImpl implements ExternalAssetOwnerSearchDataMapper {

    @Override
    public ExternalTransferData map(SearchedExternalAssetOwner source) {
        if ( source == null ) {
            return null;
        }

        ExternalTransferData externalTransferData = new ExternalTransferData();

        externalTransferData.setOwner( toOwner( source ) );
        externalTransferData.setLoan( toLoanExternalId( source ) );
        externalTransferData.setTransferExternalId( toTransferExternalId( source ) );
        externalTransferData.setStatus( toStatus( source ) );
        externalTransferData.setSubStatus( toSubStatus( source ) );
        externalTransferData.setDetails( toDetails( source ) );
        externalTransferData.setTransferId( source.getTransferId() );
        externalTransferData.setPurchasePriceRatio( source.getPurchasePriceRatio() );
        externalTransferData.setSettlementDate( source.getSettlementDate() );
        externalTransferData.setEffectiveFrom( source.getEffectiveFrom() );
        externalTransferData.setEffectiveTo( source.getEffectiveTo() );

        return externalTransferData;
    }
}
