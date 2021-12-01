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

import java.io.Serializable;

public class MessageJobResponse implements Serializable {
    protected Long jobInstanceId;
    protected Long parentId;
    protected String identifier;
    // protected String messageId;

    public MessageJobResponse(Long jobInstanceId, Long parentId, String identifier) {
        this.jobInstanceId = jobInstanceId;
        this.parentId = parentId;
        this.identifier = identifier;
    }
    public MessageJobResponse(Long jobInstanceId, String identifier) {
        this.jobInstanceId = jobInstanceId;
        this.identifier = identifier;
        this.parentId = null;
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
    public Long getParentId() {
        return parentId;
    }
    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }
}
