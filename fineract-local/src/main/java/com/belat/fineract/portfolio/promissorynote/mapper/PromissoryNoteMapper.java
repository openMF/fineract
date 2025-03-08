package com.belat.fineract.portfolio.promissorynote.mapper;

import com.belat.fineract.portfolio.promissorynote.data.PromissoryNoteData;
import com.belat.fineract.portfolio.promissorynote.domain.PromissoryNote;
import org.apache.fineract.infrastructure.core.config.MapstructMapperConfig;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.util.List;

@Mapper(config = MapstructMapperConfig.class)
public interface PromissoryNoteMapper {

    @Mapping(target = "fundSavingsAccount", ignore = true)
    @Mapping(target = "investorSavingsAccount", ignore = true)
    PromissoryNoteData map(PromissoryNote source);

    List<PromissoryNoteData> map(List<PromissoryNote> sources);

}
