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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
@Profile("kafkaEnabled")
public class KafkaProducerConfiguration {

    @Value("$FINERACT_DEFAULT_KAFKA_BOOTSTRAP_ADDRESS:localhost:9092}")
    private String bootstrapAddress;

    @Value("${FINERACT_DEFAULT_KAFKA_SECURITY_PROTOCOL:TEXTPLAIN}")
    private String securityProtocol;

    @Value("${FINERACT_DEFAULT_KAFKA_SSL_ENABLED:false}")
    private String sslEnabled;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerConfiguration.class);

    @Bean    
    @Profile("kafkaEnabled")
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "20971520");
        if (Boolean.valueOf(sslEnabled)) {
            props.put("security.protocol", securityProtocol);
        }
        LOG.info(props.toString());
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean    
    @Profile("kafkaEnabled")
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean    
    @Profile("kafkaEnabled")
    public ProducerFactory<String, NotificationData> notificationDataProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 90000);
        if (Boolean.valueOf(sslEnabled)) {
            props.put("security.protocol", securityProtocol);
        }
        LOG.info(props.toString());
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean    
    @Profile("kafkaEnabled")
    public KafkaTemplate<String, NotificationData> notificationDataKafkaTemplate() {
        return new KafkaTemplate<>(notificationDataProducerFactory());
    }

}
