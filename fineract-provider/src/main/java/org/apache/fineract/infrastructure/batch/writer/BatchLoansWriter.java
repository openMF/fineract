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

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.fineract.infrastructure.batch.config.BatchConstants;
import org.apache.fineract.infrastructure.batch.config.BatchDestinations;
import org.apache.fineract.infrastructure.batch.data.MessageBatchData;
import org.apache.fineract.infrastructure.core.utils.ProfileUtils;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

public class BatchLoansWriter implements ItemWriter<Long>, StepExecutionListener {

    public static final Logger LOG = LoggerFactory.getLogger(BatchLoansWriter.class);

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private JmsTemplate sqsJmsTemplate;

    @Autowired
    private ApplicationContext context;

    private StepExecution stepExecution;
    private String tenantIdentifier;
    private String batchJobName;
    private String queueName;

    private final Gson gson = new Gson();
    private ProfileUtils profileUtils;

    public BatchLoansWriter(BatchDestinations batchDestinations) {
        super();
        this.queueName = batchDestinations.getBatchLoansDestination();
        LOG.debug("Batch jobs communication using with the queue {}", queueName);
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.profileUtils = new ProfileUtils(context.getEnvironment());
        JobParameters parameters = stepExecution.getJobExecution().getJobParameters();
        tenantIdentifier = parameters.getString(BatchConstants.JOB_PARAM_TENANT_ID);
        batchJobName = stepExecution.getJobExecution().getJobInstance().getJobName();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(List<? extends Long> items) throws Exception {
        List<Long> batchLoanIds = new ArrayList<Long>();
        for (final Long loanId : items) {
            if (loanId != null) {
                batchLoanIds.add(loanId);
            }
        }

        MessageBatchData messageData = new MessageBatchData(batchJobName, tenantIdentifier, batchLoanIds);
        sendMessage(messageData);
    }

    private void sendMessage(final MessageBatchData message) {
        final String payload = gson.toJson(message);
        LOG.debug("Sending: {}", payload);
        // ActiveMQ
        if (profileUtils.isActiveProfile(JobConstants.SPRING_MESSAGING_PROFILE_NAME)) {
            this.jmsTemplate.send(new ActiveMQQueue(this.queueName), new MessageCreator() {

                @Override
                public Message createMessage(Session session) throws JMSException {
                    LOG.info("Sending {} to Queue {} with {} items", batchJobName, queueName, message.getEntityIds().size());
                    return session.createTextMessage(payload);
                }
            });
            // SQS
        } else if (profileUtils.isActiveProfile(JobConstants.SPRING_MESSAGINGSQS_PROFILE_NAME)) {
            LOG.info("Sending {} to Queue {} with {} items", batchJobName, queueName, message.getEntityIds().size());
            this.sqsJmsTemplate.convertAndSend(this.queueName, payload);
        }
    }
}
