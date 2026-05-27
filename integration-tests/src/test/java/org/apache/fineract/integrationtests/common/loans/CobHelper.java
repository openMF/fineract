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
package org.apache.fineract.integrationtests.common.loans;

import static org.apache.fineract.client.feign.util.FeignCalls.ok;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.models.COBPartition;
import org.apache.fineract.integrationtests.common.FineractFeignClientHelper;

@Slf4j
public final class CobHelper {

    private CobHelper() {}

    public static List<COBPartition> getCobPartitions(int partitionSize) {
        return ok(() -> FineractFeignClientHelper.getFineractFeignClient().internalCob().getCobPartitions(partitionSize));

    }

    public static void fastForwardLoansLastCOBDate(final Long loanId, final String cobDate) {
        ok(() -> {
            FineractFeignClientHelper.getFineractFeignClient().internalCob().updateLoanCobLastDate(loanId,
                    "{\"lastClosedBusinessDate\":\"" + cobDate + "\"}");
            return null;
        });
    }

    public static void reprocessLoan(final Long loanId) {
        ok(() -> {
            FineractFeignClientHelper.getFineractFeignClient().internalCob().loanReprocess(loanId);
            return null;
        });
    }
}
