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
package org.apache.fineract.infrastructure.batch.data.process;

import org.apache.fineract.infrastructure.batch.config.BatchConstants;

public class LoanTagsData {
    private Long loanId;
    private String taggedAt;
    private boolean chargedOff;
    private boolean paidOff;
    private Long accountStatus;
    private Long delinquent;

    private final String locale = BatchConstants.DEFAULT_BATCH_DATE_LOCALE;
    private final String dateFormat = BatchConstants.DEFAULT_BATCH_DATETIME_FORMAT;

    public LoanTagsData(Long loanId, String taggedAt, boolean chargedOff, boolean paidOff, 
        Long accountStatus, Long delinquent) {
        this.loanId = loanId;
        this.taggedAt = taggedAt;
        this.chargedOff = chargedOff;
        this.paidOff = paidOff;
        this.accountStatus = accountStatus;
        this.delinquent = delinquent;
    }

    public String toJson() {
        return "{\"locale\":\"" + locale
        + "\", \"dateFormat\":\"" + dateFormat
        + "\", \"tagged_at\":\"" + taggedAt
        + "\", \"charged_off\":" + chargedOff
        + ", \"paid_off\":" + paidOff
        + ", \"loan_account_status_tag_cd_account_status\":" + accountStatus
        + ", \"loan_delinquency_tag_cd_delinquent\":" + (delinquent == null ? "\"\"" : delinquent)
        + "}";
    }
}
