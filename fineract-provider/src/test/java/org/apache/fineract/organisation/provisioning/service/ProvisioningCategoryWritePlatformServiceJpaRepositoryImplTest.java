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
package org.apache.fineract.organisation.provisioning.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import org.apache.fineract.organisation.provisioning.domain.ProvisioningCategory;
import org.apache.fineract.organisation.provisioning.domain.ProvisioningCategoryRepository;
import org.apache.fineract.organisation.provisioning.exception.ProvisioningCategoryCannotBeDeletedException;
import org.apache.fineract.organisation.provisioning.exception.ProvisioningCategoryNotFoundException;
import org.apache.fineract.organisation.provisioning.serialization.ProvisioningCategoryDefinitionJsonDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.jdbc.core.JdbcTemplate;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ProvisioningCategoryWritePlatformServiceJpaRepositoryImplTest {

    @InjectMocks
    private ProvisioningCategoryWritePlatformServiceJpaRepositoryImpl underTest;

    @Mock
    private ProvisioningCategoryRepository provisioningCategoryRepository;

    @Mock
    private ProvisioningCategoryDefinitionJsonDeserializer fromApiJsonDeserializer;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private JsonCommand command;

    private static final Long CATEGORY_ID = 7L;

    @Test
    public void shouldDeleteCategoryWhenItIsNotUsedByAnyLoanProduct() {
        final ProvisioningCategory category = Mockito.mock(ProvisioningCategory.class);
        given(command.entityId()).willReturn(CATEGORY_ID);
        given(provisioningCategoryRepository.findById(CATEGORY_ID)).willReturn(Optional.of(category));
        // in-use check resolves to "false" -> not associated with any loan product
        given(jdbcTemplate.queryForObject(anyString(), eq(Boolean.class), any(Object[].class))).willReturn(false);

        final CommandProcessingResult result = underTest.deleteProvisioningCateogry(command);

        assertEquals(CATEGORY_ID, result.getResourceId());
        verify(provisioningCategoryRepository).delete(category);
        // a DELETE has no body, so create-validation must never run on this path
        verify(fromApiJsonDeserializer, never()).validateForCreate(anyString());
    }

    @Test
    public void shouldThrowNotFoundWhenCategoryDoesNotExist() {
        given(command.entityId()).willReturn(CATEGORY_ID);
        given(provisioningCategoryRepository.findById(CATEGORY_ID)).willReturn(Optional.empty());

        assertThrows(ProvisioningCategoryNotFoundException.class, () -> underTest.deleteProvisioningCateogry(command));

        verify(provisioningCategoryRepository, never()).delete(any(ProvisioningCategory.class));
    }

    @Test
    public void shouldThrowCannotBeDeletedWhenCategoryIsUsedByLoanProduct() {
        final ProvisioningCategory category = Mockito.mock(ProvisioningCategory.class);
        given(command.entityId()).willReturn(CATEGORY_ID);
        given(provisioningCategoryRepository.findById(CATEGORY_ID)).willReturn(Optional.of(category));
        // in-use check resolves to "true" -> still referenced by a loan product
        given(jdbcTemplate.queryForObject(anyString(), eq(Boolean.class), any(Object[].class))).willReturn(true);

        assertThrows(ProvisioningCategoryCannotBeDeletedException.class, () -> underTest.deleteProvisioningCateogry(command));

        verify(provisioningCategoryRepository, never()).delete(any(ProvisioningCategory.class));
    }
}
