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
package com.belat.fineract.portfolio.promissorynote.api;

import com.belat.fineract.portfolio.promissorynote.data.PromissoryNoteData;
import com.belat.fineract.portfolio.promissorynote.service.impl.PromissoryNoteReadPlatformServiceImpl;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.commands.domain.CommandWrapper;
import org.apache.fineract.commands.service.CommandWrapperBuilder;
import org.apache.fineract.commands.service.PortfolioCommandSourceWritePlatformService;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import org.apache.fineract.infrastructure.core.serialization.DefaultToApiJsonSerializer;
import org.apache.fineract.infrastructure.security.service.PlatformUserRightsContext;
import org.springframework.stereotype.Component;

@Path("/v1/promissorynote")
@Component
@Tag(name = "promissorynote", description = "PromissoryNote")
@RequiredArgsConstructor
public class PromissoryNoteApiResource {

    private final PlatformUserRightsContext platformUserRightsContext;
    private final PortfolioCommandSourceWritePlatformService commandsSourceWritePlatformService;
    private final DefaultToApiJsonSerializer<PromissoryNoteData> apiJsonSerializerService;
    private final PromissoryNoteReadPlatformServiceImpl noteReadPlatformService;

    @POST
    @Consumes({ MediaType.APPLICATION_JSON })
    @Produces({ MediaType.APPLICATION_JSON })
    @RequestBody(required = true, content = @Content(schema = @Schema(implementation = PromissoryNoteApiResourceSwagger.PostAddPromissoryNoteRequest.class)))
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "OK", content = @Content(schema = @Schema(implementation = PromissoryNoteApiResourceSwagger.PostAddPromissoryNoteResponse.class))),
            @ApiResponse(responseCode = "403", description = "Promissory can not be created") })
    public String addPromissoryNote(@Parameter(hidden = true) final String apiRequestBodyAsJson) {
        platformUserRightsContext.isAuthenticated();
        final CommandWrapper commandRequest = new CommandWrapperBuilder().withJson(apiRequestBodyAsJson).createPromissoryNote().build();
        CommandProcessingResult result = this.commandsSourceWritePlatformService.logCommandSource(commandRequest);
        return apiJsonSerializerService.serialize(result);
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "OK", content = @Content(schema = @Schema(implementation = PromissoryNoteApiResourceSwagger.PostAddPromissoryNoteRequest.class))) })
    public String getAllPromissoryNumber() {
        platformUserRightsContext.isAuthenticated();

        List<PromissoryNoteData> promissoryNotes = noteReadPlatformService.retrieveAll();

        return apiJsonSerializerService.serialize(promissoryNotes);
    }

    @GET
    @Path("{promissoryNumber}")
    @Produces({ MediaType.APPLICATION_JSON })
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "OK", content = @Content(schema = @Schema(implementation = PromissoryNoteApiResourceSwagger.PostAddPromissoryNoteRequest.class))) })
    public String getPromissoryNumber(@PathParam("promissoryNumber") final String promissoryNumber) {
        platformUserRightsContext.isAuthenticated();

        final PromissoryNoteData promissoryNoteData = noteReadPlatformService.retrieveOneByPromissoryNoteNumber(promissoryNumber);
        return apiJsonSerializerService.serialize(promissoryNoteData);
    }
}
