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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.config.BatchDestinations;
import org.apache.fineract.infrastructure.batch.data.MessageBatchDataRequest;
import org.apache.fineract.infrastructure.batch.service.BatchJobUtils;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

public class BatchLoansWriter extends BatchWriterBase implements ItemWriter<Long>, StepExecutionListener {

    public static final Logger LOG = LoggerFactory.getLogger(BatchLoansWriter.class);

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private JmsTemplate sqsJmsTemplate;

    private final static ThreadLocal<String> identifier = new ThreadLocal<>();
    private final static ThreadLocal<String> cobDate = new ThreadLocal<>();

    public BatchLoansWriter(BatchDestinations batchDestinations) {
        this.queueName = batchDestinations.getBatchLoansDestination();
        LOG.debug("Batch jobs communication using with the queue {}", queueName);
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        initialize(stepExecution);
        identifier.set(BatchJobUtils.getStringParam(parameters.get(), BatchConstants.JOB_PARAM_INSTANCE_ID));
        cobDate.set(BatchJobUtils.getStringParam(parameters.get(), BatchConstants.JOB_PARAM_COB_DATE));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        LOG.info("==={}=== {} {} with items processed {}", batchJobInstanceId.get(), batchStepName.get(),
            stepExecution.getExitStatus().getExitCode(), processed.get());
        cleanup();
        identifier.remove();
        cobDate.remove();
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(List<? extends Long> items) throws Exception {
        processed.set(processed.get() + items.size());
        sendMessage(new MessageBatchDataRequest(batchJobInstanceId.get(),
            identifier.get(), batchStepName.get(), tenantIdentifier.get(), cobDate.get(), items));
    }

    private void sendMessage(final MessageBatchDataRequest message) {
        final String payload = gson.toJson(message);
        // LOG.debug("Sending: {}", payload);
        // ActiveMQ
        if (profileUtils.isActiveProfile(JobConstants.SPRING_MESSAGING_PROFILE_NAME)) {
            chunkCounter.set(chunkCounter.get()+1);
            LOG.debug("{} shipment {} to MQ: {} items {}", batchStepName.get(), chunkCounter.get(), queueName, message.getEntityIdsQty());
            this.jmsTemplate.send(new ActiveMQQueue(this.queueName), new MessageCreator() {

                @Override
                public Message createMessage(Session session) throws JMSException {
                    return session.createTextMessage(payload);
                }
            });
            // SQS
        } else if (profileUtils.isActiveProfile(JobConstants.SPRING_MESSAGINGSQS_PROFILE_NAME)) {
            chunkCounter.set(chunkCounter.get()+1);
            LOG.debug("{} shipment {} to SQS: {} items {}", batchStepName.get(), chunkCounter.get(), queueName, message.getEntityIdsQty());
            this.sqsJmsTemplate.convertAndSend(this.queueName, payload);
        }
    }
}
