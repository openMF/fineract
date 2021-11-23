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
package org.apache.fineract.infrastructure.batch.data;

public class MessageBatchDataResults extends MessageBaseData {

    private int totalItems;
    private int processedItems;
    private int skippedItems;
    private boolean success;
    
    public MessageBatchDataResults(String tenantIdentifier, String batchStepName, int totalItems, int processedItems, int skippedItems, boolean success) {
        this.tenantIdentifier = tenantIdentifier;
        this.batchStepName = batchStepName;
        this.totalItems = totalItems;
        this.processedItems = processedItems;
        this.skippedItems = skippedItems;
        this.success = success;
    }

    public int getTotalItems() {
        return totalItems;
    }

    public void setTotalItems(int totalItems) {
        this.totalItems = totalItems;
    }

    public int getProcessedItems() {
        return processedItems;
    }

    public void setProcessedItems(int processedItems) {
        this.processedItems = processedItems;
    }

    public int getSkippedItems() {
        return skippedItems;
    }

    public void setSkippedItems(int skippedItems) {
        this.skippedItems = skippedItems;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
