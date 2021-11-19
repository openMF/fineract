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
package org.apache.fineract.portfolio.client.eventandlistener;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.fineract.infrastructure.core.domain.FineractPlatformDefaultTenant;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.security.service.BasicAuthTenantDetailsService;
import org.apache.fineract.notification.data.NotificationData;
import org.apache.fineract.notification.service.NotificationWritePlatformService;
import org.apache.fineract.useradministration.domain.AppUser;
import org.apache.fineract.useradministration.domain.AppUserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;



@Service
public class ClientCreationEventListener implements SessionAwareMessageListener {

    private final BasicAuthTenantDetailsService basicAuthTenantDetailsService;

    private final NotificationWritePlatformService notificationWritePlatformService;

    private final AppUserRepository appUserRepository;

    private static final Logger LOG = LoggerFactory.getLogger(ClientCreationEventListener.class);

    private CountDownLatch clienCreationLatch = new CountDownLatch(1);

    @Autowired      
    FineractPlatformDefaultTenant fineractPlatformDefaultTenant;

    @Autowired
    Environment environment;

    @KafkaListener(topics = "${notification.data.topic.name}", containerFactory = "notificationDataKafkaListenerContainerFactory",autoStartup = "${FINERACT_DEFAULT_KAFKA_ENABLED:false}")
    public void notificationDataListener(NotificationData clientCreationData) {
        LOG.info("LISTENER KAFKA"+ clientCreationData.toString());
        this.clienCreationLatch.countDown();

        final FineractPlatformTenant tenant; 

        if(this.fineractPlatformDefaultTenant.getFineractTenant().getTenantIdentifier().equalsIgnoreCase(clientCreationData.getTenantIdentifier()) && Arrays.stream(environment.getActiveProfiles()).anyMatch(env -> env.equalsIgnoreCase("singleTenant"))) {
            tenant = this.fineractPlatformDefaultTenant.getFineractTenant();
        } 
        else {
            tenant =this.basicAuthTenantDetailsService.loadTenantById(clientCreationData.getTenantIdentifier(),false);
            LOG.info("TENANT DB QUERY");
        }

        ThreadLocalContextUtil.setTenant(tenant);

        Long appUserId = clientCreationData.getActor();

        List<Long> userIds = clientCreationData.getUserIds();
        
        if (clientCreationData.getOfficeId() != null) {
            List<Long> tempUserIds = new ArrayList<>(userIds);
            for (Long userId : tempUserIds) {                
                AppUser appUser = appUserRepository.findById(userId).get();
                if (!Objects.equals(appUser.getOffice().getId(), clientCreationData.getOfficeId())) {
                    userIds.remove(userId);                    
                }
            }
        }
        
        if (userIds.contains(appUserId)) {
            userIds.remove(appUserId);
            LOG.info("LISTENER KAFKA userIds.remove(appUserId)");
        }
         
        if(!userIds.isEmpty()){
            notificationWritePlatformService.notify(userIds, clientCreationData.getObjectType(), clientCreationData.getObjectIdentifier(),
            clientCreationData.getAction(), clientCreationData.getActor(), clientCreationData.getContent(),
            clientCreationData.isSystemGenerated());
        } else {
            notificationWritePlatformService.notify(appUserId, clientCreationData.getObjectType(), clientCreationData.getObjectIdentifier(),
            clientCreationData.getAction(), clientCreationData.getActor(), clientCreationData.getContent(),
            clientCreationData.isSystemGenerated());
        }
        
    }

    @Autowired
    public ClientCreationEventListener(BasicAuthTenantDetailsService basicAuthTenantDetailsService,
            NotificationWritePlatformService notificationWritePlatformService, AppUserRepository appUserRepository) {
        this.basicAuthTenantDetailsService = basicAuthTenantDetailsService;
        this.notificationWritePlatformService = notificationWritePlatformService;
        this.appUserRepository = appUserRepository;
    }

    @Override
    public void onMessage(Message message, Session session) throws JMSException {
        if (message instanceof ObjectMessage) {
            LOG.info("Message arrived");
            NotificationData clientCreationData = (NotificationData) ((ObjectMessage) message).getObject();

            final FineractPlatformTenant tenant = this.basicAuthTenantDetailsService
                    .loadTenantById(clientCreationData.getTenantIdentifier(), false);
            
            ThreadLocalContextUtil.setTenant(tenant);

            Long appUserId = clientCreationData.getActor();

            List<Long> userIds = clientCreationData.getUserIds();

            if (clientCreationData.getOfficeId() != null) {
                List<Long> tempUserIds = new ArrayList<>(userIds);
                for (Long userId : tempUserIds) {
                    AppUser appUser = appUserRepository.findById(userId).get();
                    if (!Objects.equals(appUser.getOffice().getId(), clientCreationData.getOfficeId())) {
                        userIds.remove(userId);
                    }
                }
            }

            if (userIds.contains(appUserId)) {
                userIds.remove(appUserId);
            }

            if(!userIds.isEmpty()){
                notificationWritePlatformService.notify(userIds, clientCreationData.getObjectType(), clientCreationData.getObjectIdentifier(),
                clientCreationData.getAction(), clientCreationData.getActor(), clientCreationData.getContent(),
                clientCreationData.isSystemGenerated());
            } else {
                notificationWritePlatformService.notify(appUserId, clientCreationData.getObjectType(), clientCreationData.getObjectIdentifier(),
                clientCreationData.getAction(), clientCreationData.getActor(), clientCreationData.getContent(),
                clientCreationData.isSystemGenerated());
            }
        }
    }
}
