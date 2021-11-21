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
package org.apache.fineract.infrastructure.batch.listeners;

import java.util.List;
import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataRequest;
import org.apache.fineract.infrastructure.batch.service.JobRunnerImpl;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Profile({ JobConstants.SPRING_BATCH_WORKER_PROFILE_NAME, JobConstants.SPRING_MESSAGINGSQS_PROFILE_NAME })
public class BatchLoansEventSQSListener extends BatchEventBaseListener {

    private static final Logger LOG = LoggerFactory.getLogger(BatchLoansEventSQSListener.class);

    @Autowired
    private JobRunnerImpl jobRunnerImpl;

    @JmsListener(destination = "#{@batchDestinations.BatchLoansDestination}", concurrency = "#{@batchDestinations.ConcurrencyDestination}")
    public void receivedMessage(@Payload String payload) {
        // LOG.debug("receivedMessage ==== " + payload);
        MessageBatchDataRequest messageData = gson.fromJson(payload, MessageBatchDataRequest.class);
        if (messageData != null) {
            setTenant(messageData.getTenantIdentifier());
            // LOG.debug("Tenant {}", tenant.getName());

            List<Long> loanIds = getLoanIds(messageData.getEntityIds());
            LOG.debug("Job {} : {} loans", messageData.getBatchStepName(), loanIds.size());
            jobRunnerImpl.runJob(BatchConstants.BATCH_JOB_PROCESS_ID, gson.toJson(loanIds));
        }
    }
}
