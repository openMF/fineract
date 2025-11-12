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

import feign.Param;
import feign.QueryMap;
import feign.RequestLine;
import java.util.Map;
import org.apache.fineract.client.models.GetLoanProductsProductIdResponse;

public interface LoanProductsApiExtension {

    /**
     * Retrieve Loan Product Details with query parameters
     *
     * @param productId
     *            Loan Product ID
     * @param queryParams
     *            Query parameters (e.g., template=true)
     * @return GetLoanProductsProductIdResponse
     */
    @RequestLine("GET /v1/loanproducts/{productId}")
    GetLoanProductsProductIdResponse retrieveLoanProductDetails(@Param("productId") Long productId,
            @QueryMap Map<String, Object> queryParams);
}
