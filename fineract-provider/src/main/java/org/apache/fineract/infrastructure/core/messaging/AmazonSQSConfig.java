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

import java.util.Optional;
import java.util.Properties;
import javax.annotation.PostConstruct;
import javax.jms.Session;
import org.apache.fineract.infrastructure.core.utils.PropertyUtils;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.util.ErrorHandler;

@Configuration
@Profile(JobConstants.SPRING_MESSAGINGSQS_PROFILE_NAME)
@EnableJms
public class AmazonSQSConfig {

    private String regionName;
    private String concurrency;
    private String awsAccountNo;
    private Integer numberOfMessagesToPrefetch;

    private Properties messagingProperties;
    private AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private Environment environment;

    @Autowired
    private ApplicationContext context;

    @PostConstruct
    protected void init() {
        this.environment = this.context.getEnvironment();
        messagingProperties = PropertyUtils.loadYamlProperties("/sqs-messaging.yml");
        regionName = getValue("AWS_DEFAULT_REGION");
        if (regionName == null) {
            regionName = messagingProperties.getProperty("aws.messaging.region.name");
        }
        concurrency = getValue("AWS_SQS_CONCURRENCY");
        if (concurrency == null) {
            concurrency = messagingProperties.getProperty("aws.messaging.concurrency");
        }
        awsAccountNo = getValue("AWS_ACCOUNT_NO");
        if (awsAccountNo == null) {
            awsAccountNo = messagingProperties.getProperty("aws.messaging.accountno");
        }
        numberOfMessagesToPrefetch = Integer.valueOf(getValue("DEFAULT_SQS_MESSAGE_PREFETCH", "1"));
        if (numberOfMessagesToPrefetch == null)
            numberOfMessagesToPrefetch = 1;
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

    public ErrorHandler errorHandler() {
        return new DefaultErrorHandler();
    }

    public SQSConnectionFactory sqsConnectionFactory() {
        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(getRegion(regionName)).withCredentials(credentialsProvider).build();

        ProviderConfiguration providerConfiguration = new ProviderConfiguration();
            getNumberOfMessagesToPrefetch().ifPresent(providerConfiguration::setNumberOfMessagesToPrefetch);
        return new SQSConnectionFactory(providerConfiguration, sqs);
    }

    public Optional<Integer> getNumberOfMessagesToPrefetch() {
        return Optional.ofNullable(this.numberOfMessagesToPrefetch);
    }

    @Bean
    public Properties messagingProperties() {
        return this.messagingProperties;
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(sqsConnectionFactory());
        factory.setDestinationResolver(new SQSDynamicDestinationResolver(awsAccountNo));
        factory.setConcurrency(concurrency);
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        factory.setErrorHandler(errorHandler());
        return factory;
    }

    @Bean
    public JmsTemplate sqsJmsTemplate() {
        return new JmsTemplate(sqsConnectionFactory());
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
