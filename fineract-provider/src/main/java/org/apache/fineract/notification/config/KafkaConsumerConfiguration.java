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

import java.util.HashMap;
import java.util.Map;
import org.apache.fineract.notification.data.NotificationData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@EnableKafka
@Configuration
public class KafkaConsumerConfiguration {

    @Value("${fineract.kafka.bootstrapAddress:#{environment.FINERACT_DEFAULT_KAFKA_BOOTSTRAP_ADDRESS}}")
    private String bootstrapAddress;

    @Value("${fineract.kafka.securityProtocol:#{environment.FINERACT_DEFAULT_KAFKA_SECURITY_PROTOCOL}}")
    private String securityProtocol;

    @Value("${fineract.kafka.sslEnabled:#{environment.FINERACT_DEFAULT_KAFKA_SSL_ENABLED}}")
    private Boolean sslEnabled;

    @Value(value = "${kafka.group.id}")
    private String groupId;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerConfiguration.class);

    public ConsumerFactory<String, String> consumerFactory(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
        if (Boolean.TRUE.equals(sslEnabled)) {
            props.put("security.protocol", securityProtocol);
        }
        LOG.info(props.toString());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(String groupId) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(groupId));
        return factory;
    }

    public ConsumerFactory<String, NotificationData> notificationDataConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        if (Boolean.TRUE.equals(sslEnabled)) {
            props.put("security.protocol", securityProtocol);
        }
        LOG.info(props.toString());
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(NotificationData.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationData> notificationDataKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, NotificationData> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(notificationDataConsumerFactory());
        return factory;
    }

}
