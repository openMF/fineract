package com.belat.fineract.portfolio.promissorynote.service;

import com.belat.fineract.portfolio.promissorynote.data.PromissoryNoteData;

import java.util.List;

public interface PromissoryNoteReadPlatformService {

    List<PromissoryNoteData> retrieveAll();

    PromissoryNoteData retrieveOne(Long id);

    PromissoryNoteData retrieveOneByPromissoryNoteNumber(String promissoryNoteNumber);
}
