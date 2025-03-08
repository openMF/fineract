package com.belat.fineract.portfolio.promissorynote.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;

public class PromissoryNoteApiResourceSwagger {

    private PromissoryNoteApiResourceSwagger() {
    }

    @Schema(description = "PostAddPromissoryNoteRequest")
    static final class PostAddPromissoryNoteRequest {

        private PostAddPromissoryNoteRequest() {
        }

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

        private PostAddPromissoryNoteResponse() {
        }

        @Schema(example = "1")
        public Long resourceId;
    }

    @Schema(description = "GetPromissoryNoteResponse")
    static final class GetPromissoryNoteResponse {

        private GetPromissoryNoteResponse() {
        }

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
