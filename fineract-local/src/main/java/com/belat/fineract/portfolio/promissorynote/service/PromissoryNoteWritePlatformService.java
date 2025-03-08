package com.belat.fineract.portfolio.promissorynote.service;

import com.belat.fineract.portfolio.promissorynote.domain.PromissoryNote;
import com.google.gson.JsonElement;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;

public interface PromissoryNoteWritePlatformService {

    PromissoryNote createPromissoryNote(JsonElement element);

    void validatePromissoryNoteRequestBody(final JsonCommand command);

    CommandProcessingResult addPromissoryNote(JsonCommand command);
}
