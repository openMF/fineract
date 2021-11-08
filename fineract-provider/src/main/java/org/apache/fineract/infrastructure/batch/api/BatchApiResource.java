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
package org.apache.fineract.infrastructure.batch.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.apache.fineract.infrastructure.batch.service.JobRunner;
import org.apache.fineract.infrastructure.core.exception.UnrecognizedQueryParamException;
import org.apache.fineract.infrastructure.core.utils.TextUtils;
import org.apache.fineract.infrastructure.security.exception.NoAuthorizationException;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Path("/batch")
@Component
@Scope("singleton")
@Tag(name = "Batch API", description = "The Apache Fineract Batch API enables to run batch jobs using Spring Batch")
public class BatchApiResource {

    private final PlatformSecurityContext context;
    private final JobRunner jobRunner;

    /**
     * Constructs a 'BatchApiService' with context, toApiJsonSerializer, service and batchRequestJsonHelper.
     *
     * @param context
     */
    @Autowired
    public BatchApiResource(final PlatformSecurityContext context, final JobRunner jobRunner) {
        this.context = context;
        this.jobRunner = jobRunner;
    }

    @POST
    @Path("{jobId}")
    @Operation(summary = "Run a Job", description = "Manually Execute Specific Job.")
    @ApiResponses({ @ApiResponse(responseCode = "200", description = "POST: jobs/1?command=executeJob") })
    public Response executeBatchJob(@PathParam("jobId") @Parameter(description = "jobId") final Long jobId,
            @QueryParam("command") @Parameter(description = "command") final String commandParam) {
        final boolean hasNotPermission = this.context.authenticatedUser().hasNotPermissionForAnyOf("ALL_FUNCTIONS", "EXECUTEJOB_SCHEDULER");
        if (hasNotPermission) {
            final String authorizationMessage = "User has no authority to execute scheduler jobs";
            throw new NoAuthorizationException(authorizationMessage);
        }
        Response response = Response.status(400).build();
        if (TextUtils.is(commandParam, "start")) {
            jobRunner.runJob(jobId);
            response = Response.status(202).build();
        } else {
            throw new UnrecognizedQueryParamException("command", commandParam);
        }
        return response;
    }
}
