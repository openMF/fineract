package com.belat.fineract.portfolio.promissorynote.handler;

import lombok.RequiredArgsConstructor;
import org.apache.fineract.commands.annotation.CommandType;
import org.apache.fineract.commands.handler.NewCommandSourceHandler;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import com.belat.fineract.portfolio.promissorynote.service.impl.PromissoryNoteWritePlatformServiceImpl;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@CommandType(entity = "PROMISSORY_NOTE", action = "CREATE")
public class AddPromissoryNoteCommandHandler implements NewCommandSourceHandler {

    private final PromissoryNoteWritePlatformServiceImpl promissoryNoteWritePlatformService;

    @Override
    public CommandProcessingResult processCommand(JsonCommand command) {
       return promissoryNoteWritePlatformService.addPromissoryNote(command);
    }
}
