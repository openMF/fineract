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
package org.apache.fineract.infrastructure.batch.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

public class ItemCounterListener implements ChunkListener {

    public static final Logger LOG = LoggerFactory.getLogger(ItemCounterListener.class);
    private int chunkCounter = 0;

    @Override
    public void beforeChunk(ChunkContext context) {}

    @Override
    public void afterChunk(ChunkContext context) {
        int count = context.getStepContext().getStepExecution().getReadCount();
        LOG.info("Context: {}, Items: {}", ++chunkCounter, count);
    }

    @Override
    public void afterChunkError(ChunkContext context) {}
}
