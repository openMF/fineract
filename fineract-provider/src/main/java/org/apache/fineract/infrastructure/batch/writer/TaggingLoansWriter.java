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
package org.apache.fineract.infrastructure.batch.writer;

import java.util.List;

import org.apache.fineract.infrastructure.batch.config.BatchDestinations;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResponse;
import org.apache.fineract.infrastructure.batch.data.MessageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;

public class TaggingLoansWriter extends BatchWriterBase implements ItemWriter<MessageBatchDataResponse>, StepExecutionListener {

    public static final Logger LOG = LoggerFactory.getLogger(TaggingLoansWriter.class);

    public TaggingLoansWriter(BatchDestinations batchDestinations) {
        super();
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        initialize(stepExecution);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        LOG.info("{} items processed {}", this.batchStepName, this.processed);
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(List<? extends MessageBatchDataResponse> items) throws Exception {
        for (final MessageBatchDataResponse processResult : items) {
            if (processResult.wasChanged()) {
                MessageData messageData = new MessageData(processResult.getBatchStepName(), processResult.getTenantIdentifier(), 
                    processResult.getEntityId(), processResult.getPayload());
                sendMessage(messageData);
                this.processed++;
            }
        }
    }

    private void sendMessage(final MessageData message) {
        // final String payload = gson.toJson(message);
        // LOG.debug("Sending notification {}: {}", this.batchStepName, payload);
    }
}
