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

import org.apache.fineract.notification.data.NotificationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class NotificationKafkaEventService {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationKafkaEventService.class);
  
    @Autowired
    private final KafkaTemplate<String, NotificationData> notificationDataKafkaTemplate;

    @Autowired
    public NotificationKafkaEventService(KafkaTemplate<String, NotificationData> kafkaTemplate) {
        this.notificationDataKafkaTemplate = kafkaTemplate;
    }
    
    public void broadcastNotificationKafka(final String destination, final NotificationData notificationData) {

        StringBuilder key = new StringBuilder();
        key.append(notificationData.getOfficeId()).append(notificationData.getObjectIdentifier());

        ListenableFuture<SendResult<String, NotificationData>> future = 
            this.notificationDataKafkaTemplate.send(destination,key.toString(), notificationData);

        future.addCallback(new ListenableFutureCallback<SendResult<String, NotificationData>>() {

            @Override
            public void onSuccess(SendResult<String, NotificationData> result) {
                LOG.info("Sent notificationData message=[" + notificationData.toString() + " with Id "
                        + notificationData.getObjectIdentifier() + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                LOG.error("Unable to send notificationData message=[" + notificationData + "] due to : " + ex.getMessage());
            }
        });
    }
}
