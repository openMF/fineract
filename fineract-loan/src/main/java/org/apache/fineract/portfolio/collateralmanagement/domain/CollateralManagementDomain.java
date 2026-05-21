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
package org.apache.fineract.portfolio.collateralmanagement.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.fineract.infrastructure.core.domain.AbstractPersistableCustom;
import org.apache.fineract.organisation.monetary.domain.ApplicationCurrency;

@Entity
@Table(name = "m_collateral_management")
@Getter
@Setter
public class CollateralManagementDomain extends AbstractPersistableCustom<Long> {

    @Column(name = "name", length = 20, columnDefinition = " ")
    private String name;

    @Column(name = "quality", nullable = false, length = 40)
    private String quality;

    @Column(name = "base_price", nullable = false, scale = 5, precision = 20)
    private BigDecimal basePrice;

    @Column(name = "unit_type", nullable = false, length = 10)
    private String unitType;

    @Column(name = "pct_to_base", nullable = false, scale = 5, precision = 20)
    private BigDecimal pctToBase;

    @ManyToOne
    @JoinColumn(name = "currency")
    private ApplicationCurrency currency;

    @OneToMany(mappedBy = "collateral", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @Setter(AccessLevel.NONE)
    private Set<ClientCollateralManagement> clientCollateralManagements = new HashSet<>();

    protected CollateralManagementDomain() {
        // for JPA
    }

    @Builder
    private CollateralManagementDomain(final String name, final String quality, final BigDecimal basePrice, final String unitType,
            final BigDecimal pctToBase, final ApplicationCurrency currency) {
        this.name = name;
        this.quality = quality;
        this.basePrice = basePrice;
        this.unitType = unitType;
        this.pctToBase = pctToBase;
        this.currency = currency;
    }
}
