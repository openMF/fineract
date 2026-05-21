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
package org.apache.fineract.portfolio.collateralmanagement.service;

import java.math.BigDecimal;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.organisation.monetary.domain.ApplicationCurrency;
import org.apache.fineract.organisation.monetary.domain.ApplicationCurrencyRepository;
import org.apache.fineract.portfolio.collateralmanagement.data.CollateralProductCreateRequest;
import org.apache.fineract.portfolio.collateralmanagement.data.CollateralProductCreateResponse;
import org.apache.fineract.portfolio.collateralmanagement.data.CollateralProductDeleteRequest;
import org.apache.fineract.portfolio.collateralmanagement.data.CollateralProductDeleteResponse;
import org.apache.fineract.portfolio.collateralmanagement.data.CollateralProductUpdateRequest;
import org.apache.fineract.portfolio.collateralmanagement.data.CollateralProductUpdateResponse;
import org.apache.fineract.portfolio.collateralmanagement.data.CollateralProductUpdateResponse.Changes;
import org.apache.fineract.portfolio.collateralmanagement.domain.ClientCollateralManagement;
import org.apache.fineract.portfolio.collateralmanagement.domain.CollateralManagementDomain;
import org.apache.fineract.portfolio.collateralmanagement.domain.CollateralManagementRepositoryWrapper;
import org.apache.fineract.portfolio.collateralmanagement.exception.CollateralCannotBeDeletedException;
import org.apache.fineract.portfolio.collateralmanagement.exception.CollateralNotFoundException;

@RequiredArgsConstructor
public class CollateralManagementWriteServiceImpl implements CollateralManagementWriteService {

    private final CollateralManagementRepositoryWrapper collateralManagementRepositoryWrapper;
    private final ApplicationCurrencyRepository applicationCurrencyRepository;

    @Override
    public CollateralProductCreateResponse createCollateralProduct(final CollateralProductCreateRequest request) {
        final ApplicationCurrency applicationCurrency = this.applicationCurrencyRepository.findOneByCode(request.getCurrency());
        final CollateralManagementDomain collateral = CollateralManagementDomain.builder() //
                .name(request.getName()) //
                .quality(request.getQuality()) //
                .basePrice(request.getBasePrice()) //
                .unitType(request.getUnitType()) //
                .pctToBase(request.getPctToBase()) //
                .currency(applicationCurrency) //
                .build();
        this.collateralManagementRepositoryWrapper.create(collateral);

        return CollateralProductCreateResponse.builder().resourceId(collateral.getId()).build();
    }

    @Override
    public CollateralProductUpdateResponse updateCollateralProduct(final CollateralProductUpdateRequest request) {
        final CollateralManagementDomain collateral = this.collateralManagementRepositoryWrapper.getCollateral(request.getCollateralId());
        final ApplicationCurrency applicationCurrency = request.getCurrency() != null
                ? this.applicationCurrencyRepository.findOneByCode(request.getCurrency())
                : collateral.getCurrency();
        final Changes changes = applyChanges(collateral, request, applicationCurrency);
        this.collateralManagementRepositoryWrapper.update(collateral);

        return CollateralProductUpdateResponse.builder().resourceId(request.getCollateralId()).changes(changes).build();
    }

    private static Changes applyChanges(final CollateralManagementDomain collateral, final CollateralProductUpdateRequest request,
            final ApplicationCurrency applicationCurrency) {
        final Changes.ChangesBuilder changes = Changes.builder();

        if (request.getName() != null && !Objects.equals(collateral.getName(), request.getName())) {
            collateral.setName(request.getName().isEmpty() ? null : request.getName());
            changes.name(collateral.getName());
        }
        if (request.getQuality() != null && !Objects.equals(collateral.getQuality(), request.getQuality())) {
            collateral.setQuality(request.getQuality());
            changes.quality(collateral.getQuality());
        }
        if (request.getUnitType() != null && !Objects.equals(collateral.getUnitType(), request.getUnitType())) {
            collateral.setUnitType(request.getUnitType());
            changes.unitType(collateral.getUnitType());
        }

        collateral.setCurrency(applicationCurrency);

        if (request.getBasePrice() != null
                && (collateral.getBasePrice() == null || collateral.getBasePrice().compareTo(request.getBasePrice()) != 0)) {
            collateral.setBasePrice(request.getBasePrice());
            changes.basePrice(collateral.getBasePrice());
        }
        if (request.getPctToBase() != null
                && (collateral.getPctToBase() == null || collateral.getPctToBase().compareTo(request.getPctToBase()) != 0)) {
            collateral.setPctToBase(request.getPctToBase());
            changes.pctToBase(collateral.getPctToBase());
        }

        return changes.build();
    }

    @Override
    public CollateralProductDeleteResponse deleteCollateralProduct(final CollateralProductDeleteRequest request) {
        final CollateralManagementDomain collateral = this.collateralManagementRepositoryWrapper.getCollateral(request.getCollateralId());
        validateForDeletion(collateral, request.getCollateralId());
        this.collateralManagementRepositoryWrapper.delete(request.getCollateralId());

        return CollateralProductDeleteResponse.builder().resourceId(request.getCollateralId()).build();
    }

    private void validateForDeletion(final CollateralManagementDomain collateralManagementDomain, final Long collateralId) {
        if (collateralManagementDomain == null) {
            throw new CollateralNotFoundException(collateralId);
        }

        if (!collateralManagementDomain.getClientCollateralManagements().isEmpty()) {
            for (ClientCollateralManagement clientCollateralManagement : collateralManagementDomain.getClientCollateralManagements()) {
                if (clientCollateralManagement.getQuantity().compareTo(BigDecimal.ZERO) > 0) {
                    throw new CollateralCannotBeDeletedException(
                            CollateralCannotBeDeletedException.CollateralCannotBeDeletedReason.COLLATERAL_IS_ALREADY_ATTACHED,
                            collateralId);
                }
            }
        }
    }
}
