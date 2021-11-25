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
package org.apache.fineract.infrastructure.batch.service;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.security.service.TenantDetailsService;

public class BatchJobUtils {

    public static FineractPlatformTenant setTenant(final String tenantIdentifier, final TenantDetailsService tenantDetailsService) {
        final FineractPlatformTenant tenant = tenantDetailsService.loadTenantById(tenantIdentifier);
        ThreadLocalContextUtil.setTenant(tenant);
        return tenant;
    }

    public static List<Long> getLoanIds(Object obj, Gson gson) {
        List<Long> items = new ArrayList<Long>();
        if (obj instanceof List) {
            for (int i = 0; i < ((List<?>) obj).size(); i++) {
                Object item = ((List<?>) obj).get(i);
                items.add(gson.fromJson(gson.toJson(item), Long.class));
            }
        }
        return items;
    }

    public static List<Long> getLoanIds(String str) {
        str = str.replace("[", "");
        str = str.replace("]", "");
        str = str.replace(", ", ",");
        return Arrays.stream(str.split(",")).map(Long::valueOf).collect(Collectors.toList());
    }
}
