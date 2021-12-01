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

package org.apache.fineract.infrastructure.core.messaging;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import java.util.Properties;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.jms.Session;
import org.apache.fineract.infrastructure.core.utils.PropertyUtils;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;

@Configuration
@Profile({ JobConstants.SPRING_BATCH_PROFILE_NAME, JobConstants.SPRING_MESSAGINGSQS_PROFILE_NAME })
public class AmazonSQSConfig {

    public static final Logger LOG = LoggerFactory.getLogger(AmazonSQSConfig.class);

    private String regionName;
    private String concurrency;
    private String awsAccountNo;
    private Integer numberOfMessagesToPrefetch;
    private Integer threadPoolSize;

    private Properties messagingProperties;
    private AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private Environment environment;

    @Autowired
    private ApplicationContext context;

    private SQSConnectionFactory connectionFactory;
    private AmazonSQS sqsClient;

    @PostConstruct
    protected void init() {
        this.environment = this.context.getEnvironment();
        messagingProperties = PropertyUtils.loadYamlProperties("/sqs-messaging.yml");
        regionName = getValue("AWS_DEFAULT_REGION");
        if (regionName == null) {
            regionName = messagingProperties.getProperty("aws.messaging.region.name");
        }
        concurrency = getValue("DEFAULT_SQS_CONCURRENCY", "1");
        if (concurrency == null) {
            concurrency = messagingProperties.getProperty("aws.messaging.concurrency");
        }
        LOG.info("SQS concurrency {}", concurrency);
        awsAccountNo = getValue("AWS_ACCOUNT_NO");
        if (awsAccountNo == null) {
            awsAccountNo = messagingProperties.getProperty("aws.messaging.accountno");
        }
        numberOfMessagesToPrefetch = Integer.valueOf(getValue("DEFAULT_SQS_MESSAGE_PREFETCH", "1"));
        if (numberOfMessagesToPrefetch == null)
            numberOfMessagesToPrefetch = 1;
        LOG.info("SQS Number of Messages prefetch {}", numberOfMessagesToPrefetch);

        threadPoolSize = Integer.valueOf(getValue("DEFAULT_SQS_THREAD_POOL", "1"));
        if (threadPoolSize == null)
            threadPoolSize = 1;
        LOG.info("SQS Thread pool size {}", threadPoolSize);

        this.connectionFactory = sqsConnectionFactory();
    }

    private String getValue(final String key, final String defaultVal) {
        String propertyVal = this.environment.getProperty(key);
        if (propertyVal == null)
            return defaultVal;
        return propertyVal;
    }

    private String getValue(final String key) {
        return this.environment.getProperty(key);
    }

    public SQSConnectionFactory sqsConnectionFactory() {
        this.sqsClient = AmazonSQSClientBuilder.standard().withRegion(getRegion(regionName)).withCredentials(credentialsProvider).build();
    
        ProviderConfiguration providerConfiguration = new ProviderConfiguration();
        providerConfiguration.setNumberOfMessagesToPrefetch(numberOfMessagesToPrefetch);
        return new SQSConnectionFactory(providerConfiguration, this.sqsClient);
    }

    @Bean
    public AmazonSQS sqsClient() {
        return this.sqsClient;
    }

    @Bean
    public Properties messagingProperties() {
        return this.messagingProperties;
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(this.connectionFactory);
        factory.setDestinationResolver(new SQSDynamicDestinationResolver(awsAccountNo));
        factory.setConcurrency(concurrency);
        factory.setMaxMessagesPerTask(numberOfMessagesToPrefetch);
        factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        factory.setErrorHandler(new DefaultErrorHandler());
        factory.setTaskExecutor(Executors.newFixedThreadPool(threadPoolSize));
        return factory;
    }

    @Bean
    public JmsTemplate sqsJmsTemplate() {
        return new JmsTemplate(this.connectionFactory);
    }

    private Regions getRegion(String regionName) {
        switch (regionName) {
            case "us-east-1":
                return Regions.US_EAST_1;
            case "us-east-2":
                return Regions.US_EAST_2;
            default:
                return Regions.DEFAULT_REGION;
        }
    }
}
