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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.destination.DestinationResolver;
import org.springframework.util.Assert;

public class SQSDynamicDestinationResolver implements DestinationResolver {

    public static final Logger LOG = LoggerFactory.getLogger(SQSDynamicDestinationResolver.class);

    private String ownerAccountId;

    public SQSDynamicDestinationResolver(String ownerAccountId) {
        this.ownerAccountId = ownerAccountId;
    }

    @Override
    public Destination resolveDestinationName(Session session, String destinationName, boolean pubSubDomain) throws JMSException {
        Assert.notNull(session, "Session must not be null");
        Assert.notNull(destinationName, "Destination name must not be null");
        if (pubSubDomain) {
            return resolveTopic(session, destinationName);
        } else {
            return resolveQueue(session, destinationName);
        }
    }

    protected Topic resolveTopic(Session session, String topicName) throws JMSException {
        return session.createTopic(topicName);
    }

    protected Queue resolveQueue(Session session, String queueName) throws JMSException {
        Queue queue;
        LOG.debug("Getting destination for ownerAccountId: {}, queueName: {}", ownerAccountId, queueName);

        queue = session.createQueue(queueName);

        return queue;
    }
}
