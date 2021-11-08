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
import java.util.Properties;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.fineract.infrastructure.batch.config.BatchConfiguration;
import org.apache.fineract.infrastructure.core.utils.PropertyUtils;
import org.apache.fineract.portfolio.loanaccount.loanschedule.data.OverdueLoanScheduleData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

public class ApplyChargeForOverdueLoansWriter implements ItemWriter<OverdueLoanScheduleData> {

    public static final Logger LOG = LoggerFactory.getLogger(ApplyChargeForOverdueLoansWriter.class);

    @Autowired
    private JmsTemplate jmsTemplate;

    private Properties batchJobProperties;
    private Queue queue;

    public ApplyChargeForOverdueLoansWriter() {
        super();
        this.batchJobProperties = PropertyUtils.loadYamlProperties(BatchConfiguration.BATCH_PROPERTIES);

        String queueName = batchJobProperties.getProperty("fineract.batch.jobs.queues.applyChargeToOverdueLoans");
        String communication = batchJobProperties.getProperty("fineract.batch.jobs.communication");
        LOG.info("Batch jobs communication using {} with the queue {}", communication, queueName);

        LOG.info("Queue name: " + queueName);
        if (communication.equalsIgnoreCase("activemq"))
            queue = new ActiveMQQueue(queueName);
        else
            queue = new ActiveMQQueue(queueName);
    }

    @Override
    public void write(List<? extends OverdueLoanScheduleData> items) throws Exception {
        for (OverdueLoanScheduleData item : items) {
            LOG.info("Sending: " + item.toString());
            this.jmsTemplate.send(queue, new MessageCreator() {

                @Override
                public Message createMessage(Session session) throws JMSException {
                    return session.createObjectMessage(item);
                }
            });
        }
    }
}
