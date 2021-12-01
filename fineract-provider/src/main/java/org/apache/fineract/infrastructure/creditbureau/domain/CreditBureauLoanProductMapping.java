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
package org.apache.fineract.infrastructure.creditbureau.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.domain.AbstractPersistableCustom;
import org.apache.fineract.portfolio.loanproduct.domain.LoanProduct;

@SuppressWarnings({ "MemberName" })
@Entity
@Table(name = "m_creditbureau_loanproduct_mapping")
public class CreditBureauLoanProductMapping extends AbstractPersistableCustom {

    @Column(name = "is_creditcheck_mandatory")
    private boolean isCreditCheckMandatory;

    @Column(name = "skip_creditcheck_in_failure")
    private boolean skipCreditCheckInFailure;

    @Column(name = "stale_period")
    private int stalePeriod;

    @Column(name = "is_active")
    private boolean isActive;

    @ManyToOne
    private OrganisationCreditBureau organisation_creditbureau;

    @OneToOne
    @JoinColumn(name = "loan_product_id")
    private LoanProduct loanProduct;

    public CreditBureauLoanProductMapping() {

    }

    public CreditBureauLoanProductMapping(boolean isCreditCheckMandatory, boolean skipCreditCheckInFailure, int stalePeriod,
            boolean isActive, OrganisationCreditBureau organisationCreditbureau, LoanProduct loanProduct) {
        this.isCreditCheckMandatory = isCreditCheckMandatory;
        this.skipCreditCheckInFailure = skipCreditCheckInFailure;
        this.stalePeriod = stalePeriod;
        this.isActive = isActive;
        this.organisation_creditbureau = organisationCreditbureau;
        this.loanProduct = loanProduct;
    }

    public static CreditBureauLoanProductMapping fromJson(final JsonCommand command, OrganisationCreditBureau organisation_creditbureau,
            LoanProduct loanProduct) {
        boolean isCreditCheckMandatory = false;
        boolean skipCreditCheckInFailure = false;
        Integer stalePeriod = -1;
        boolean isActive = false;
        if (command.booleanPrimitiveValueOfParameterNamed("isCreditcheckMandatory")) {
            isCreditCheckMandatory = command.booleanPrimitiveValueOfParameterNamed("isCreditcheckMandatory");
        }

        if (command.booleanPrimitiveValueOfParameterNamed("skipCreditcheckInFailure")) {
            skipCreditCheckInFailure = command.booleanPrimitiveValueOfParameterNamed("skipCreditcheckInFailure");
        }

        if (command.integerValueOfParameterNamed("stalePeriod") != null) {
            stalePeriod = command.integerValueOfParameterNamed("stalePeriod");
        }

        if (command.booleanPrimitiveValueOfParameterNamed("isActive")) {
            isActive = command.booleanPrimitiveValueOfParameterNamed("isActive");
        }

        return new CreditBureauLoanProductMapping(isCreditCheckMandatory, skipCreditCheckInFailure, stalePeriod, isActive,
                organisation_creditbureau, loanProduct);

    }

    public boolean isCreditCheckMandatory() {
        return this.isCreditCheckMandatory;
    }

    public void setCreditCheckMandatory(boolean isCreditCheckMandatory) {
        this.isCreditCheckMandatory = isCreditCheckMandatory;
    }

    public boolean isSkipCreditCheckInFailure() {
        return this.skipCreditCheckInFailure;
    }

    public void setSkipCreditCheckInFailure(boolean skipCreditCheckInFailure) {
        this.skipCreditCheckInFailure = skipCreditCheckInFailure;
    }

    public int getStalePeriod() {
        return this.stalePeriod;
    }

    public void setStalePeriod(int stalePeriod) {
        this.stalePeriod = stalePeriod;
    }

    public boolean getIsActive() {
        return this.isActive;
    }

    public void setIsActive(boolean isActive) {
        this.isActive = isActive;
    }

    public OrganisationCreditBureau getOrganisationCreditbureau() {
        return this.organisation_creditbureau;
    }

    public LoanProduct getLoanProduct() {
        return this.loanProduct;
    }
}