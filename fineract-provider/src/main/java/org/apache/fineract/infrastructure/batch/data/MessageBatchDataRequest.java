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

import java.util.List;

public class MessageBatchDataRequest extends MessageBaseData {

    private Long jobInstanceId;
    private String identifier;
    private String cobDate;
    private List<? extends Long> entityIds;

    public MessageBatchDataRequest(Long jobInstanceId, String identifier, String batchStepName, 
        String tenantIdentifier, String cobDate, List<? extends Long> entityIds) {
        this.jobInstanceId = jobInstanceId;
        this.identifier = identifier;
        this.cobDate = cobDate;
        this.batchStepName = batchStepName;
        this.tenantIdentifier = tenantIdentifier;
        this.entityIds = entityIds;
    }

    public Integer getEntityIdsQty() {
        if (entityIds == null)
            return 0;
        return entityIds.size();
    }

    public List<? extends Long> getEntityIds() {
        return entityIds;
    }

    public void setEntityIds(List<Long> entityIds) {
        this.entityIds = entityIds;
    }

    public String getEntityIdsAsString() {
        String entityIdsVal = this.entityIds.toString();
        return entityIdsVal.substring(1, entityIdsVal.length() - 1);    
    }

    public Long getJobInstanceId() {
        return jobInstanceId;
    }

    public void setJobInstanceId(Long jobInstanceId) {
        this.jobInstanceId = jobInstanceId;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getCobDate() {
        return cobDate;
    }

    public void setCobDate(String cobDate) {
        this.cobDate = cobDate;
    }
}
