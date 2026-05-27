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
package org.apache.fineract.integrationtests.common;

import static org.apache.fineract.client.feign.util.FeignCalls.ok;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.models.GetSearchResponse;

@Slf4j
public final class SearchHelper {

    private SearchHelper() {}

    public static List<GetSearchResponse> getSearch(final String query, final Boolean exactMatch, final String resources) {
        log.info("Searching: query={}, exactMatch={}, resources={}", query, exactMatch, resources);
        return ok(() -> FineractFeignClientHelper.getFineractFeignClient().search().searchData(query, resources, exactMatch));
    }
}
