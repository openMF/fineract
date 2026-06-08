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
package org.apache.fineract.integrationtests.client.feign.helpers;

import static org.apache.fineract.client.feign.util.FeignCalls.ok;

import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.models.PostSavingsAccountTransactionsRequest;
import org.apache.fineract.client.models.PostSavingsAccountTransactionsResponse;
import org.apache.fineract.integrationtests.client.feign.modules.SavingsRequestBuilders;

public class FeignSavingsTransactionHelper {

    private final FineractFeignClient fineractClient;

    public FeignSavingsTransactionHelper(FineractFeignClient fineractClient) {
        this.fineractClient = fineractClient;
    }

    public PostSavingsAccountTransactionsResponse deposit(Long savingsId, PostSavingsAccountTransactionsRequest request) {
        return ok(() -> fineractClient.savingsAccountTransactions().createSavingsAccountTransaction(savingsId, request, "deposit"));
    }

    public PostSavingsAccountTransactionsResponse deposit(Long savingsId, String amount, String transactionDate) {
        return deposit(savingsId, SavingsRequestBuilders.deposit(amount, transactionDate));
    }

    public PostSavingsAccountTransactionsResponse withdraw(Long savingsId, PostSavingsAccountTransactionsRequest request) {
        return ok(() -> fineractClient.savingsAccountTransactions().createSavingsAccountTransaction(savingsId, request, "withdrawal"));
    }

    public PostSavingsAccountTransactionsResponse withdraw(Long savingsId, String amount, String transactionDate) {
        return withdraw(savingsId, SavingsRequestBuilders.withdrawal(amount, transactionDate));
    }
}
