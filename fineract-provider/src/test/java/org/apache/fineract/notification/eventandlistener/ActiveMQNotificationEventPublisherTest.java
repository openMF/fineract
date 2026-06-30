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
package org.apache.fineract.notification.eventandlistener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import jakarta.jms.Destination;
import org.apache.fineract.notification.data.NotificationData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@ExtendWith(MockitoExtension.class)
class ActiveMQNotificationEventPublisherTest {

    @Mock
    private JmsTemplate jmsTemplate;

    @InjectMocks
    private ActiveMQNotificationEventPublisher underTest;

    @Test
    void shouldSendImmediatelyWhenNoActiveTransaction() {
        NotificationData notificationData = new NotificationData();

        underTest.broadcastNotification(notificationData);

        verify(jmsTemplate).send(any(Destination.class), any(MessageCreator.class));
    }

    @Test
    void shouldDeferSendingUntilAfterCommitWhenTransactionIsActive() {
        NotificationData notificationData = new NotificationData();

        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            underTest.broadcastNotification(notificationData);

            // Should not have sent yet
            verify(jmsTemplate, never()).send(any(Destination.class), any(MessageCreator.class));

            // Trigger afterCommit manually
            TransactionSynchronizationManager.getSynchronizations().forEach(s -> s.afterCommit());

            verify(jmsTemplate).send(any(Destination.class), any(MessageCreator.class));
        } finally {
            TransactionSynchronizationManager.setActualTransactionActive(false);
            TransactionSynchronizationManager.clearSynchronization();
        }
    }
}
