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
package org.apache.fineract.integrationtests.common.accounting;

import static org.apache.fineract.client.feign.util.FeignCalls.ok;

import org.apache.fineract.client.models.PostRunaccrualsRequest;
import org.apache.fineract.integrationtests.common.FineractFeignClientHelper;

public final class PeriodicAccrualAccountingHelper {

    private PeriodicAccrualAccountingHelper() {}

    public static void runPeriodicAccrualAccounting(final String date) {
        final PostRunaccrualsRequest request = new PostRunaccrualsRequest().dateFormat("dd MMMM yyyy").locale("en_GB").tillDate(date);
        ok(() -> {
            FineractFeignClientHelper.getFineractFeignClient().periodicAccrualAccounting().executePeriodicAccrualAccounting(request);
            return null;
        });
    }
}
