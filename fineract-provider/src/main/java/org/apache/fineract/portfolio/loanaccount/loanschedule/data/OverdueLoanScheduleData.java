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
package org.apache.fineract.portfolio.loanaccount.loanschedule.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;

public class OverdueLoanScheduleData implements Serializable {

    private Long loanId;
    private Long chargeId;
    private String locale;
    private BigDecimal amount;
    private String dateFormat;
    private String dueDate;
    private BigDecimal principalOverdue;
    private BigDecimal interestOverdue;
    private Integer periodNumber;

    public OverdueLoanScheduleData(final Long loanId, final Long chargeId, final String dueDate, final BigDecimal amount,
            final String dateFormat, final String locale, final BigDecimal principalOverdue, final BigDecimal interestOverdue,
            final Integer periodNumber) {
        this.loanId = loanId;
        this.chargeId = chargeId;
        this.dueDate = dueDate;
        this.amount = amount;
        this.dateFormat = dateFormat;
        this.locale = locale;
        this.principalOverdue = principalOverdue;
        this.interestOverdue = interestOverdue;
        this.periodNumber = periodNumber;
    }

    @Override
    public String toString() {
        return "{" + "chargeId:" + this.chargeId + ", locale:'" + this.locale + '\'' + ", amount:" + this.amount + ", dateFormat:'"
                + this.dateFormat + '\'' + ", dueDate:'" + this.dueDate + '\'' + ", principal:'" + this.principalOverdue + '\''
                + ", interest:'" + this.interestOverdue + '\'' + '}';
    }

    public static OverdueLoanScheduleData fromJSON(String json) throws JsonProcessingException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(json, OverdueLoanScheduleData.class);
    }

    public Long getLoanId() {
        return loanId;
    }

    public void setLoanId(Long loanId) {
        this.loanId = loanId;
    }

    public Long getChargeId() {
        return chargeId;
    }

    public void setChargeId(Long chargeId) {
        this.chargeId = chargeId;
    }

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getDueDate() {
        return dueDate;
    }

    public void setDueDate(String dueDate) {
        this.dueDate = dueDate;
    }

    public BigDecimal getPrincipalOverdue() {
        return principalOverdue;
    }

    public void setPrincipalOverdue(BigDecimal principalOverdue) {
        this.principalOverdue = principalOverdue;
    }

    public BigDecimal getInterestOverdue() {
        return interestOverdue;
    }

    public void setInterestOverdue(BigDecimal interestOverdue) {
        this.interestOverdue = interestOverdue;
    }

    public Integer getPeriodNumber() {
        return periodNumber;
    }

    public void setPeriodNumber(Integer periodNumber) {
        this.periodNumber = periodNumber;
    }
}
