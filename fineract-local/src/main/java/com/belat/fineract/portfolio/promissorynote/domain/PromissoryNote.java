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

package com.belat.fineract.portfolio.promissorynote.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.fineract.infrastructure.core.domain.AbstractAuditableWithUTCDateTimeCustom;
import org.apache.fineract.portfolio.savings.domain.SavingsAccount;

@Getter
@Setter
@Entity
@NoArgsConstructor
@Table(name = "e_promissory_note")
public class PromissoryNote extends AbstractAuditableWithUTCDateTimeCustom<Long> {

    @Enumerated(EnumType.ORDINAL)
    @Column(name = "status_enum", nullable = false)
    private PromissoryNoteStatus status;

    @ToString.Exclude
    @OneToOne
    @JoinColumn(name = "fund_savings_account_id", nullable = false, referencedColumnName = "id")
    private SavingsAccount fundSavingsAccount;

    @ToString.Exclude
    @OneToOne
    @JoinColumn(name = "investor_savings_account_id", nullable = false, referencedColumnName = "id")
    private SavingsAccount investorSavingsAccount;

    @Column(name = "investment_amount", nullable = false)
    private BigDecimal investmentAmount;

    @Column(name = "promissory_note_number", length = 20, unique = true, nullable = false)
    private String promissoryNoteNumber;

    @Column(name = "currency_code", nullable = false)
    private String currencyCode;

}
