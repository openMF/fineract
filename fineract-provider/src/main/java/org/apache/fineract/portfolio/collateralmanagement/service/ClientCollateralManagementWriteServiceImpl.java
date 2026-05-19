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
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.core.data.ApiParameterError;
import org.apache.fineract.infrastructure.core.data.DataValidatorBuilder;
import org.apache.fineract.infrastructure.core.exception.PlatformApiDataValidationException;
import org.apache.fineract.portfolio.client.domain.Client;
import org.apache.fineract.portfolio.client.domain.ClientRepositoryWrapper;
import org.apache.fineract.portfolio.collateralmanagement.data.ClientCollateralCreateRequest;
import org.apache.fineract.portfolio.collateralmanagement.data.ClientCollateralCreateResponse;
import org.apache.fineract.portfolio.collateralmanagement.data.ClientCollateralDeleteRequest;
import org.apache.fineract.portfolio.collateralmanagement.data.ClientCollateralDeleteResponse;
import org.apache.fineract.portfolio.collateralmanagement.data.ClientCollateralUpdateRequest;
import org.apache.fineract.portfolio.collateralmanagement.data.ClientCollateralUpdateResponse;
import org.apache.fineract.portfolio.collateralmanagement.domain.ClientCollateralManagement;
import org.apache.fineract.portfolio.collateralmanagement.domain.ClientCollateralManagementRepositoryWrapper;
import org.apache.fineract.portfolio.collateralmanagement.domain.CollateralManagementDomain;
import org.apache.fineract.portfolio.collateralmanagement.domain.CollateralManagementRepositoryWrapper;
import org.apache.fineract.portfolio.collateralmanagement.exception.ClientCollateralCannotBeDeletedException;
import org.apache.fineract.portfolio.collateralmanagement.exception.ClientCollateralNotFoundException;
import org.apache.fineract.portfolio.collateralmanagement.exception.CollateralNotFoundException;
import org.apache.fineract.portfolio.loanaccount.domain.LoanCollateralManagement;

@RequiredArgsConstructor
public class ClientCollateralManagementWriteServiceImpl implements ClientCollateralManagementWriteService {

    private final ClientCollateralManagementRepositoryWrapper clientCollateralManagementRepositoryWrapper;
    private final CollateralManagementRepositoryWrapper collateralManagementRepositoryWrapper;
    private final ClientRepositoryWrapper clientRepositoryWrapper;

    private static final String COLLATERAL_ID = "collateralId";
    private static final String QUANTITY = "quantity";

    @Override
    public ClientCollateralCreateResponse createClientCollateralProduct(final ClientCollateralCreateRequest request) {
        validateForCreation(request);

        final Client client = this.clientRepositoryWrapper.findOneWithNotFoundDetection(request.getClientId(), false);
        final CollateralManagementDomain collateralManagementData = this.collateralManagementRepositoryWrapper
                .getCollateral(request.getCollateralId());
        final ClientCollateralManagement clientCollateralManagement = ClientCollateralManagement.createNew(request.getQuantity(), client,
                collateralManagementData);
        this.clientCollateralManagementRepositoryWrapper.saveAndFlush(clientCollateralManagement);

        return ClientCollateralCreateResponse.builder().resourceId(clientCollateralManagement.getId()).clientId(request.getClientId())
                .build();
    }

    @Override
    public ClientCollateralUpdateResponse updateClientCollateralProduct(final ClientCollateralUpdateRequest request) {
        validateForUpdate(request);

        final ClientCollateralManagement collateral = this.clientCollateralManagementRepositoryWrapper
                .getCollateral(request.getCollateralId());
        final BigDecimal oldQuantity = collateral.getQuantity();
        boolean changed = false;
        if (request.getQuantity() != null && request.getQuantity().compareTo(oldQuantity) != 0) {
            collateral.updateQuantity(request.getQuantity());
            changed = true;
        }
        this.clientCollateralManagementRepositoryWrapper.updateClientCollateralProduct(collateral);

        ClientCollateralUpdateResponse.ClientCollateralUpdateResponseBuilder builder = ClientCollateralUpdateResponse.builder()
                .resourceId(request.getCollateralId()).clientId(request.getClientId());
        if (changed) {
            builder.changes(
                    ClientCollateralUpdateResponse.Changes.builder().quantity(request.getQuantity()).locale(request.getLocale()).build());
        }
        return builder.build();
    }

    @Override
    public ClientCollateralDeleteResponse deleteClientCollateralProduct(final ClientCollateralDeleteRequest request) {
        final ClientCollateralManagement clientCollateralManagement = this.clientCollateralManagementRepositoryWrapper
                .getCollateral(request.getCollateralId());
        validateForDeletion(clientCollateralManagement, request.getCollateralId());
        this.clientCollateralManagementRepositoryWrapper.deleteClientCollateralProduct(request.getCollateralId());

        return ClientCollateralDeleteResponse.builder().resourceId(request.getCollateralId()).build();
    }

    private void validateForCreation(final ClientCollateralCreateRequest request) {
        final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
        final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("client-collateral");

        if (request.getCollateralId() == null) {
            baseDataValidator.reset().parameter(COLLATERAL_ID).failWithCode("parameter.collateralId.not.exists");
        }

        if (request.getQuantity() == null) {
            baseDataValidator.reset().parameter(QUANTITY).failWithCode("parameter.quantity.not.exists");
        } else {
            baseDataValidator.reset().parameter(QUANTITY).value(request.getQuantity()).notNull().positiveAmount();
        }

        if (!dataValidationErrors.isEmpty()) {
            throw new PlatformApiDataValidationException(dataValidationErrors);
        }
    }

    private void validateForUpdate(final ClientCollateralUpdateRequest request) {
        final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
        final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("client-collateral");

        if (request.getQuantity() == null) {
            baseDataValidator.reset().parameter(QUANTITY).failWithCode("parameter..quantity.not.exists");
        } else {
            baseDataValidator.reset().parameter(QUANTITY).value(request.getQuantity()).notNull().positiveAmount();
        }

        final ClientCollateralManagement clientCollateralManagement = this.clientCollateralManagementRepositoryWrapper
                .getCollateral(request.getCollateralId());

        if (clientCollateralManagement == null) {
            throw new ClientCollateralNotFoundException(request.getCollateralId());
        }

        BigDecimal totalQuantity = BigDecimal.ZERO;
        if (!clientCollateralManagement.getLoanCollateralManagementSet().isEmpty()) {
            for (LoanCollateralManagement loanCollateralManagement : clientCollateralManagement.getLoanCollateralManagementSet()) {
                totalQuantity = totalQuantity.add(loanCollateralManagement.getQuantity());
            }
        }

        if (totalQuantity.compareTo(request.getQuantity()) >= 0) {
            baseDataValidator.reset().parameter(QUANTITY).value(request.getQuantity()).notLessThanMin(totalQuantity);
        }

        if (!dataValidationErrors.isEmpty()) {
            throw new PlatformApiDataValidationException(dataValidationErrors);
        }
    }

    private void validateForDeletion(final ClientCollateralManagement clientCollateralManagement, final Long clientCollateralId) {
        if (clientCollateralManagement == null) {
            throw new CollateralNotFoundException(clientCollateralId);
        }

        if (!clientCollateralManagement.getLoanCollateralManagementSet().isEmpty()) {
            for (LoanCollateralManagement loanCollateralManagement : clientCollateralManagement.getLoanCollateralManagementSet()) {
                if (!loanCollateralManagement.isReleased()) {
                    throw new ClientCollateralCannotBeDeletedException(
                            ClientCollateralCannotBeDeletedException.ClientCollateralCannotBeDeletedReason.CLIENT_COLLATERAL_IS_ALREADY_ATTACHED,
                            clientCollateralId);
                }
            }
        }
    }
}
