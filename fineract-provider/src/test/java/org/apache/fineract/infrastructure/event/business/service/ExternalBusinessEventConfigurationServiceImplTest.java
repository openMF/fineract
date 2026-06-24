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
package org.apache.fineract.infrastructure.event.business.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.lang.reflect.Method;
import org.apache.fineract.infrastructure.event.business.domain.BusinessEvent;
import org.apache.fineract.infrastructure.event.external.repository.ExternalEventConfigurationRepository;
import org.apache.fineract.infrastructure.event.external.repository.domain.ExternalEventConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.annotation.Transactional;

@ExtendWith(MockitoExtension.class)
class ExternalBusinessEventConfigurationServiceImplTest {

    @Mock
    private ExternalEventConfigurationRepository eventConfigurationRepository;

    @InjectMocks
    private ExternalBusinessEventConfigurationServiceImpl underTest;

    @Test
    void shouldReturnFalseForNullEventWithoutTouchingTheDatabase() {
        assertThat(underTest.isExternalEventConfiguredForPosting(null)).isFalse();
        verifyNoInteractions(eventConfigurationRepository);
    }

    @Test
    void shouldReturnConfiguredEnabledFlag() {
        BusinessEvent<?> event = mock(BusinessEvent.class);
        given(event.getType()).willReturn("LoanApprovedBusinessEvent");
        ExternalEventConfiguration configuration = mock(ExternalEventConfiguration.class);
        given(configuration.isEnabled()).willReturn(true);
        given(eventConfigurationRepository.findExternalEventConfigurationByTypeWithNotFoundDetection("LoanApprovedBusinessEvent"))
                .willReturn(configuration);

        assertThat(underTest.isExternalEventConfiguredForPosting(event)).isTrue();
    }

    @Test
    void shouldReturnFalseWhenConfigurationIsDisabled() {
        BusinessEvent<?> event = mock(BusinessEvent.class);
        given(event.getType()).willReturn("LoanApprovedBusinessEvent");
        ExternalEventConfiguration configuration = mock(ExternalEventConfiguration.class);
        given(configuration.isEnabled()).willReturn(false);
        given(eventConfigurationRepository.findExternalEventConfigurationByTypeWithNotFoundDetection("LoanApprovedBusinessEvent"))
                .willReturn(configuration);

        assertThat(underTest.isExternalEventConfiguredForPosting(event)).isFalse();
    }

    /**
     * The configuration read is the path that previously leaked a connection. It must run inside a real, committing
     * (read-only) transaction so the connection is acquired, the query runs and the connection is released immediately,
     * rather than being held idle-in-transaction.
     */
    @Test
    void configurationLookupMustRunInAReadOnlyTransaction() throws NoSuchMethodException {
        Method method = ExternalBusinessEventConfigurationServiceImpl.class.getMethod("isExternalEventConfiguredForPosting",
                BusinessEvent.class);
        Transactional transactional = method.getAnnotation(Transactional.class);

        assertThat(transactional).as("isExternalEventConfiguredForPosting must be @Transactional").isNotNull();
        assertThat(transactional.readOnly()).as("the configuration read must be marked read-only").isTrue();
    }

    /**
     * Guards against re-introducing the leak: {@code notifyPostBusinessEvent} must NOT carry a
     * {@code @Transactional(SUPPORTS)} annotation, which established a synchronization scope that held a connection for
     * the whole request.
     */
    @Test
    void notifyPostBusinessEventMustNotBeTransactional() throws NoSuchMethodException {
        Method method = BusinessEventNotifierServiceImpl.class.getMethod("notifyPostBusinessEvent", BusinessEvent.class);

        assertThat(method.getAnnotation(Transactional.class)).as("notifyPostBusinessEvent must not be @Transactional(SUPPORTS)").isNull();
    }
}
