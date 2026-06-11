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
package org.apache.fineract.infrastructure.security.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;
import org.apache.fineract.infrastructure.core.config.FineractProperties;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.security.exception.OidcUserNotFoundException;
import org.apache.fineract.organisation.office.domain.Office;
import org.apache.fineract.organisation.office.domain.OfficeRepository;
import org.apache.fineract.useradministration.domain.AppUser;
import org.apache.fineract.useradministration.domain.AppUserRepository;
import org.apache.fineract.useradministration.domain.RoleRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OidcAppUserResolutionServiceImplTest {

    @Mock
    private AppUserRepository appUserRepository;
    @Mock
    private RoleRepository roleRepository;
    @Mock
    private OfficeRepository officeRepository;
    @Mock
    private FineractProperties fineractProperties;
    @Mock
    private FineractProperties.FineractSecurityProperties securityProperties;
    @Mock
    private FineractProperties.FineractSecurityProperties.FineractSecurityOidcFederationProperties oidcProps;
    @Mock
    private FineractProperties.FineractDefaultValues defaultProps;
    @Mock
    private AppUser existingUser;
    @Mock
    private Office headOffice;
    @Mock
    private FineractPlatformTenant platformTenant;

    @InjectMocks
    private OidcAppUserResolutionServiceImpl service;

    @BeforeEach
    void setUp() {
        when(fineractProperties.getDefaults()).thenReturn(defaultProps);
        when(defaultProps.getOfficeId()).thenReturn(1L);
        when(fineractProperties.getSecurity()).thenReturn(securityProperties);
        when(securityProperties.getOidcFederation()).thenReturn(oidcProps);
        // Required by AppUser constructor via DateUtils.getLocalDateOfTenant()
        when(platformTenant.getTimezoneId()).thenReturn("UTC");
        ThreadLocalContextUtil.setTenant(platformTenant);
    }

    @AfterEach
    void tearDown() {
        ThreadLocalContextUtil.reset();
    }

    @Test
    void returnsExistingUserFoundByUsername() {
        when(appUserRepository.findAppUserByName("alice")).thenReturn(existingUser);

        AppUser result = service.resolveOrCreate("alice", "alice@example.com", "Alice", "Smith", Set.of());

        assertThat(result).isSameAs(existingUser);
        verify(appUserRepository, never()).findActiveUserByEmail(any());
        verify(appUserRepository, never()).saveAndFlush(any());
    }

    @Test
    void fallsBackToEmailWhenUsernameNotFound() {
        when(appUserRepository.findAppUserByName("alice")).thenReturn(null);
        when(appUserRepository.findActiveUserByEmail("alice@example.com")).thenReturn(existingUser);

        AppUser result = service.resolveOrCreate("alice", "alice@example.com", "Alice", "Smith", Set.of());

        assertThat(result).isSameAs(existingUser);
        verify(appUserRepository, never()).saveAndFlush(any());
    }

    @Test
    void throwsOidcUserNotFoundWhenUserMissingAndAutoCreateDisabled() {
        when(appUserRepository.findAppUserByName("ghost")).thenReturn(null);
        when(appUserRepository.findActiveUserByEmail("ghost@example.com")).thenReturn(null);
        when(oidcProps.isAutoCreateUser()).thenReturn(false);

        assertThatThrownBy(() ->
                service.resolveOrCreate("ghost", "ghost@example.com", "Ghost", "User", Set.of()))
                .isInstanceOf(OidcUserNotFoundException.class)
                .extracting(e -> ((OidcUserNotFoundException) e).getSubject())
                .isEqualTo("ghost");
    }

    @Test
    void autoCreatesUserWhenEnabledAndUserNotFound() {
        when(appUserRepository.findAppUserByName("newuser")).thenReturn(null);
        when(appUserRepository.findActiveUserByEmail("new@example.com")).thenReturn(null);
        when(oidcProps.isAutoCreateUser()).thenReturn(true);
        when(oidcProps.getDefaultRoles()).thenReturn("");
        when(officeRepository.findById(1L)).thenReturn(Optional.of(headOffice));

        AppUser savedUser = org.mockito.Mockito.mock(AppUser.class);
        when(appUserRepository.saveAndFlush(any(AppUser.class))).thenReturn(savedUser);

        AppUser result = service.resolveOrCreate("newuser", "new@example.com", "New", "User", Set.of());

        assertThat(result).isSameAs(savedUser);
        verify(appUserRepository).saveAndFlush(any(AppUser.class));
    }

    @Test
    void throwsWhenHeadOfficeNotFoundDuringAutoCreate() {
        when(appUserRepository.findAppUserByName("newuser")).thenReturn(null);
        when(appUserRepository.findActiveUserByEmail(any())).thenReturn(null);
        when(oidcProps.isAutoCreateUser()).thenReturn(true);
        when(officeRepository.findById(1L)).thenReturn(Optional.empty());

        assertThatThrownBy(() ->
                service.resolveOrCreate("newuser", "new@example.com", "New", "User", Set.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Head office");
    }

    @Test
    void skipsEmailLookupWhenEmailIsNull() {
        when(appUserRepository.findAppUserByName("alice")).thenReturn(null);
        when(oidcProps.isAutoCreateUser()).thenReturn(false);

        assertThatThrownBy(() ->
                service.resolveOrCreate("alice", null, "Alice", "Smith", Set.of()))
                .isInstanceOf(OidcUserNotFoundException.class);

        verify(appUserRepository, never()).findActiveUserByEmail(any());
    }
}
