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
package org.apache.fineract.notification.config;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.apache.fineract.notification.eventandlistener.NotificationEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

@Configuration
public class MessagingConfiguration {

    @Autowired
    private NotificationEventListener notificationEventListener;

    @Bean
    public Logger loggerBean() {
        return LoggerFactory.getLogger(MessagingConfiguration.class);
    }

    private static final String DEFAULT_BROKER_URL = "tcp://localhost:61616";

    public Properties activeMqProperties() throws IOException {
        PropertiesFactoryBean propertiesFactoryBean = new PropertiesFactoryBean();
        propertiesFactoryBean.setLocation(new ClassPathResource("/activemq.properties"));
        propertiesFactoryBean.afterPropertiesSet();
        return propertiesFactoryBean.getObject();
    }

    @Bean
    public ActiveMQConnectionFactory amqConnectionFactory() {
        ActiveMQConnectionFactory amqConnectionFactory = new ActiveMQConnectionFactory();
        try {
            final Properties activeMqProperties = activeMqProperties();
            amqConnectionFactory.setBrokerURL(activeMqProperties.getProperty("activemq.broker.url"));
            amqConnectionFactory.setUserName(activeMqProperties.getProperty("spring.activemq.user"));
            amqConnectionFactory.setPassword(activeMqProperties.getProperty("spring.activemq.password"));
            amqConnectionFactory.setTrustedPackages(Arrays.asList("org.apache.fineract", "java.util", "java.lang", "org.apache.fineract",
                    "java.math", "org.apache.fineract.notification.data"));
        } catch (Exception e) {
            amqConnectionFactory.setBrokerURL(DEFAULT_BROKER_URL);
        }
        return amqConnectionFactory;
    }

    @Bean
    public CachingConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(amqConnectionFactory());
        return connectionFactory;
    }

    @Bean
    public JmsTemplate jmsTemplate() {
        JmsTemplate jmsTemplate;
        jmsTemplate = new JmsTemplate(connectionFactory());
        jmsTemplate.setConnectionFactory(connectionFactory());
        return jmsTemplate;
    }

    @Bean
    public DefaultMessageListenerContainer messageListenerContainer() {
        DefaultMessageListenerContainer messageListenerContainer = new DefaultMessageListenerContainer();
        messageListenerContainer.setConnectionFactory(connectionFactory());
        messageListenerContainer.setDestinationName("NotificationQueue");
        messageListenerContainer.setMessageListener(notificationEventListener);
        messageListenerContainer.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException jmse) {
                loggerBean().error("Network Error: ActiveMQ Broker Unavailable.");
                messageListenerContainer.shutdown();
            }
        });
        return messageListenerContainer;
    }

    @Bean
    @Profile(JobConstants.SPRING_MESSAGING_PROFILE_NAME)
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(amqConnectionFactory());
        return factory;
    }
}
