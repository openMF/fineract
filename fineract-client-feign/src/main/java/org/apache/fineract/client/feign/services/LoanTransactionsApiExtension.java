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
package org.apache.fineract.client.feign.services;

import feign.HeaderMap;
import feign.Param;
import feign.QueryMap;
import feign.RequestLine;
import java.util.Map;
import org.apache.fineract.client.models.PostLoansLoanIdTransactionsRequest;
import org.apache.fineract.client.models.PostLoansLoanIdTransactionsResponse;

public interface LoanTransactionsApiExtension {

    @RequestLine("POST /v1/loans/{loanId}/transactions")
    PostLoansLoanIdTransactionsResponse executeLoanTransaction(@Param("loanId") Long loanId,
            PostLoansLoanIdTransactionsRequest postLoansLoanIdTransactionsRequest, @QueryMap Map<String, Object> queryParams,
            @HeaderMap Map<String, String> headers);

    @RequestLine("POST /v1/loans/external-id/{loanExternalId}/transactions")
    PostLoansLoanIdTransactionsResponse executeLoanTransaction1(@Param("loanExternalId") String loanExternalId,
            PostLoansLoanIdTransactionsRequest postLoansLoanIdTransactionsRequest, @QueryMap Map<String, Object> queryParams,
            @HeaderMap Map<String, String> headers);

}
