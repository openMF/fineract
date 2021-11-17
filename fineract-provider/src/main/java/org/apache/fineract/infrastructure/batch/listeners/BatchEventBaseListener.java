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

import com.google.gson.Gson;
import java.util.List;
import org.apache.fineract.infrastructure.batch.config.BatchDestinations;
import org.apache.fineract.infrastructure.batch.service.BatchJobUtils;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.security.service.TenantDetailsService;
import org.springframework.beans.factory.annotation.Autowired;

public class BatchEventBaseListener {

    @Autowired
    protected TenantDetailsService tenantDetailsService;

    @Autowired
    protected BatchDestinations batchDestinations;

    protected final Gson gson = new Gson();

    public FineractPlatformTenant setTenant(final String tenantIdentifier) {
        return BatchJobUtils.setTenant(tenantIdentifier, this.tenantDetailsService);
    }

    public List<Long> getLoanIds(Object obj) {
        return BatchJobUtils.getLoanIds(obj, gson);
    }
}
