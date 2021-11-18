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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
@Profile("kafkaEnabled")
public class KafkaConfiguration {

    @Value("${environment.FINERACT_DEFAULT_KAFKA_BOOTSTRAP_ADDRESS:localhost:9092}")
    private String bootstrapAddress;

    @Value("${environment.FINERACT_DEFAULT_KAFKA_SECURITY_PROTOCOL:TEXTPLAIN}")
    private String securityProtocol;

    @Value("${environment.FINERACT_DEFAULT_KAFKA_SSL_ENABLED:false}")
    private String sslEnabled;

    @Value(value = "${notification.data.topic.name:notificationDataTopic}")
    private String notificationDataTopicName;

    @Value(value = "${client.creation.topic.name:clientCreationTopic}")
    private String clientCreationTopicName;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfiguration.class);

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        if (Boolean.valueOf(sslEnabled)) {
            configs.put("security.protocol", securityProtocol);
        }
        LOG.info(configs.toString());
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic notificationDataTopic() {
        return new NewTopic(notificationDataTopicName, 1, (short) 1);
    }

    @Bean
    public NewTopic clientCreationTopic() {
        return new NewTopic(clientCreationTopicName, 1, (short) 1);
    }

}
