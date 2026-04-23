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
package org.apache.fineract.infrastructure.security.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.fineract.infrastructure.security.api.AuthenticationApiResource.AuthenticateRequest;
import org.apache.fineract.infrastructure.security.data.AuthenticatedUserData;
import org.apache.fineract.infrastructure.security.exception.PasswordResetRequiredException;
import org.apache.fineract.infrastructure.security.service.SpringSecurityPlatformSecurityContext;
import org.apache.fineract.infrastructure.core.data.EnumOptionData;
import org.apache.fineract.infrastructure.core.serialization.ToApiJsonSerializer;
import org.apache.fineract.organisation.office.domain.Office;
import org.apache.fineract.useradministration.domain.AppUser;
import org.apache.fineract.useradministration.domain.Role;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class AuthenticationApiResourceTest {

    @Mock
    private DaoAuthenticationProvider authenticationProvider;
    @Mock
    private ToApiJsonSerializer<AuthenticatedUserData> apiJsonSerializerService;
    @Mock
    private SpringSecurityPlatformSecurityContext securityContext;
    @Mock
    private Authentication authenticationResult;
    @Mock
    private AppUser principal;
    @Mock
    private Office office;

    private AuthenticationApiResource resource;

    @BeforeEach
    void setUp() {
        resource = new AuthenticationApiResource(authenticationProvider, apiJsonSerializerService, securityContext);
        ReflectionTestUtils.setField(resource, "twoFactorEnabled", false);
    }

    // --- guard-rail tests (FINERACT-726 null checks) ---

    @Test
    void authenticate_nullRequest_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> resource.authenticate(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void authenticate_nullUsername_throwsIllegalArgumentException() {
        AuthenticateRequest request = new AuthenticateRequest();
        request.password = "secret";

        assertThatThrownBy(() -> resource.authenticate(request))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void authenticate_nullPassword_throwsIllegalArgumentException() {
        AuthenticateRequest request = new AuthenticateRequest();
        request.username = "mifos";

        assertThatThrownBy(() -> resource.authenticate(request))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- happy-path: no 2FA, no password-reset ---

    @Test
    void authenticate_validCredentials_returnsSerializedUserData() {
        AuthenticateRequest request = validRequest();
        stubSuccessfulAuth(/* twoFactorEnabled */ false, /* passwordResetRequired */ false);
        when(apiJsonSerializerService.serialize(any())).thenReturn("{\"authenticated\":true}");

        String result = resource.authenticate(request);

        assertThat(result).isEqualTo("{\"authenticated\":true}");
    }

    // --- 2FA required ---

    @Test
    void authenticate_twoFactorRequired_setsFlag() {
        ReflectionTestUtils.setField(resource, "twoFactorEnabled", true);
        AuthenticateRequest request = validRequest();

        // principal does NOT have the bypass-2fa permission → 2FA is required
        stubSuccessfulAuth(/* twoFactorEnabled */ true, /* passwordResetRequired */ false);
        when(principal.hasSpecificPermissionTo(any())).thenReturn(false);
        when(apiJsonSerializerService.serialize(any(AuthenticatedUserData.class))).thenAnswer(invocation -> {
            AuthenticatedUserData data = invocation.getArgument(0);
            assertThat(data.isTwoFactorAuthenticationRequired()).isTrue();
            return "{}";
        });

        resource.authenticate(request);
    }

    @Test
    void authenticate_twoFactorBypassPermission_doesNotRequire2FA() {
        ReflectionTestUtils.setField(resource, "twoFactorEnabled", true);
        AuthenticateRequest request = validRequest();

        stubSuccessfulAuth(/* twoFactorEnabled */ true, /* passwordResetRequired */ false);
        when(principal.hasSpecificPermissionTo(any())).thenReturn(true);  // has bypass permission
        when(apiJsonSerializerService.serialize(any(AuthenticatedUserData.class))).thenAnswer(invocation -> {
            AuthenticatedUserData data = invocation.getArgument(0);
            assertThat(data.isTwoFactorAuthenticationRequired()).isFalse();
            return "{}";
        });

        resource.authenticate(request);
    }

    // --- password-reset required ---

    @Test
    void authenticate_passwordResetRequired_throwsPasswordResetRequiredException() {
        AuthenticateRequest request = validRequest();
        stubSuccessfulAuth(/* twoFactorEnabled */ false, /* passwordResetRequired */ true);

        assertThatThrownBy(() -> resource.authenticate(request))
                .isInstanceOf(PasswordResetRequiredException.class)
                .extracting(ex -> ((PasswordResetRequiredException) ex).getAuthenticatedUserData())
                .satisfies(data -> {
                    assertThat(data.isAuthenticated()).isTrue();
                    assertThat(data.isShouldRenewPassword()).isTrue();
                });
    }

    // --- helpers ---

    private AuthenticateRequest validRequest() {
        AuthenticateRequest request = new AuthenticateRequest();
        request.username = "mifos";
        request.password = "password";
        return request;
    }

    private void stubSuccessfulAuth(boolean twoFactorEnabled, boolean passwordResetRequired) {
        when(authenticationProvider.authenticate(any())).thenReturn(authenticationResult);
        when(authenticationResult.isAuthenticated()).thenReturn(true);

        GrantedAuthority authority = mock(GrantedAuthority.class);
        when(authority.getAuthority()).thenReturn("ALL_FUNCTIONS");
        when(authenticationResult.getAuthorities()).thenAnswer(inv -> List.of(authority));
        when(authenticationResult.getPrincipal()).thenReturn(principal);

        when(principal.getId()).thenReturn(1L);
        when(principal.getOffice()).thenReturn(office);
        when(office.getId()).thenReturn(10L);
        when(office.getName()).thenReturn("Head Office");
        when(principal.getStaffId()).thenReturn(null);
        when(principal.getStaffDisplayName()).thenReturn(null);
        when(principal.organisationalRoleData()).thenReturn(mock(EnumOptionData.class));
        when(principal.getRoles()).thenReturn(Set.of(mock(Role.class)));

        if (twoFactorEnabled) {
            // hasSpecificPermissionTo is set by individual tests
        } else {
            // When 2FA is disabled the field value (false) short-circuits the &&,
            // so hasSpecificPermissionTo is never called — no stub needed.
        }

        when(securityContext.doesPasswordHasToBeRenewed(principal)).thenReturn(passwordResetRequired);
    }
}
