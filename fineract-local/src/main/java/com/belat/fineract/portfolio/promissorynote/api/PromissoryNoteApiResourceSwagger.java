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
package com.belat.fineract.portfolio.promissorynote.api;

import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;

public class PromissoryNoteApiResourceSwagger {

    private PromissoryNoteApiResourceSwagger() {}

    @Schema(description = "PostAddPromissoryNoteRequest")
    static final class PostAddPromissoryNoteRequest {

        private PostAddPromissoryNoteRequest() {}

        @Schema(example = "1")
        public String fundSavingsAccountId;

        @Schema(example = "2")
        public String investorSavingsAccountId;

        @Schema(example = "1000.00")
        public BigDecimal amount;

        @Schema(example = "1234567890")
        public String promissoryNoteNumber;

        @Schema(example = "1")
        public Integer status;
    }

    @Schema(description = "PostAddPromissoryNoteResponse")
    static final class PostAddPromissoryNoteResponse {

        private PostAddPromissoryNoteResponse() {}

        @Schema(example = "1")
        public Long resourceId;
    }

    @Schema(description = "GetPromissoryNoteResponse")
    static final class GetPromissoryNoteResponse {

        private GetPromissoryNoteResponse() {}

        @Schema(example = "1")
        public Long id;

        @Schema(example = "1")
        public String fundSavingsAccountId;

        @Schema(example = "2")
        public String investorSavingsAccountId;

        @Schema(example = "1000.00")
        public BigDecimal amount;

        @Schema(example = "1234567890")
        public String promissoryNoteNumber;

        @Schema(example = "1")
        public Integer status;
    }

}
