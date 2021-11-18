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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.fineract.infrastructure.batch.data.MessageData;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.apache.fineract.portfolio.loanaccount.loanschedule.data.OverdueLoanScheduleData;
import org.apache.fineract.portfolio.loanaccount.service.LoanWritePlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.stereotype.Component;

@Component
@Profile({ JobConstants.SPRING_BATCH_WORKER_PROFILE_NAME, JobConstants.SPRING_MESSAGING_PROFILE_NAME })
public class ApplyChargeForOverdueLoanEventListener extends BatchEventBaseListener implements SessionAwareMessageListener<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(ApplyChargeForOverdueLoanEventListener.class);

    @Autowired
    private LoanWritePlatformService loanWritePlatformService;

    @Override
    @JmsListener(destination = "#{@batchDestinations.ApplyChargeToOverdueLoanDestination}", concurrency = "#{@batchDestinations.ConcurrencyDestination}")
    public void onMessage(Message message, Session session) {
        try {
            LOG.debug("onMessage ==== " + message.getJMSMessageID());
            if (message instanceof TextMessage) {
                String payload = ((TextMessage) message).getText();
                MessageData messageData = gson.fromJson(payload, MessageData.class);
                if (messageData != null) {
                    final Long loanId = messageData.getEntityId();
                    final FineractPlatformTenant tenant = setTenant(messageData.getTenantIdentifier());
                    LOG.debug("Tenant {}", tenant.getName());

                    Collection<OverdueLoanScheduleData> loanData = getItems(messageData.getPayload());
                    LOG.debug("Job {} to Loan Id {} : {} items", messageData.getBatchJobName(), loanId, loanData.size());
                    this.loanWritePlatformService.applyOverdueChargesForLoan(loanId, loanData);
                }
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage());
        }
    }

    public List<OverdueLoanScheduleData> getItems(Object obj) {
        List<OverdueLoanScheduleData> items = new ArrayList<OverdueLoanScheduleData>();
        if (obj instanceof List) {
            for (int i = 0; i < ((List<?>) obj).size(); i++) {
                Object item = ((List<?>) obj).get(i);
                items.add(gson.fromJson(gson.toJson(item), OverdueLoanScheduleData.class));
            }
        }
        return items;
    }
}
