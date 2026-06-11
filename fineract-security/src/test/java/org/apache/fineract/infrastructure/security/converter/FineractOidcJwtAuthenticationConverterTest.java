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
package org.apache.fineract.infrastructure.security.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.apache.fineract.infrastructure.security.data.FineractJwtAuthenticationToken;
import org.apache.fineract.infrastructure.security.exception.OidcUserNotFoundException;
import org.apache.fineract.infrastructure.security.service.FineractOidcUserService;
import org.apache.fineract.useradministration.domain.AppUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.jwt.Jwt;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FineractOidcJwtAuthenticationConverterTest {

    @Mock
    private FineractOidcUserService oidcUserService;

    @InjectMocks
    private FineractOidcJwtAuthenticationConverter converter;

    private Jwt jwt;
    private AppUser appUser;

    @BeforeEach
    void setUp() {
        jwt = Jwt.withTokenValue("token").header("alg", "RS256").subject("sub-123").claim("preferred_username", "alice")
                .issuedAt(Instant.now()).expiresAt(Instant.now().plusSeconds(3600)).build();

        appUser = mock(AppUser.class);
        when(appUser.getAuthorities()).thenReturn(List.of());
    }

    @Test
    void returnsTokenWithAppUserAsPrincipal() {
        when(oidcUserService.extractUsername(jwt)).thenReturn("alice");
        when(oidcUserService.resolveUser(jwt, "alice")).thenReturn(appUser);

        FineractJwtAuthenticationToken token = converter.convert(jwt);

        assertThat(token).isNotNull();
        assertThat(token.getPrincipal()).isSameAs(appUser);
    }

    @Test
    void wrapsOidcUserNotFoundAsOAuth2AuthenticationException() {
        when(oidcUserService.extractUsername(jwt)).thenReturn("unknown");
        when(oidcUserService.resolveUser(jwt, "unknown"))
                .thenThrow(new OidcUserNotFoundException("unknown"));

        assertThatThrownBy(() -> converter.convert(jwt))
                .isInstanceOf(OAuth2AuthenticationException.class)
                .hasCauseInstanceOf(OidcUserNotFoundException.class);
    }

    @Test
    void tokenCarriesAuthoritiesFromAppUser() {
        var authority = mock(org.springframework.security.core.GrantedAuthority.class);
        when(authority.getAuthority()).thenReturn("READ_LOAN");
        when(appUser.getAuthorities()).thenReturn(List.of(authority));

        when(oidcUserService.extractUsername(jwt)).thenReturn("alice");
        when(oidcUserService.resolveUser(jwt, "alice")).thenReturn(appUser);

        FineractJwtAuthenticationToken token = converter.convert(jwt);

        assertThat(token.getAuthorities()).extracting(org.springframework.security.core.GrantedAuthority::getAuthority)
                .contains("READ_LOAN");
    }

    @Test
    void tokenCarriesOriginalJwt() {
        when(oidcUserService.extractUsername(jwt)).thenReturn("alice");
        when(oidcUserService.resolveUser(jwt, "alice")).thenReturn(appUser);

        FineractJwtAuthenticationToken token = converter.convert(jwt);

        assertThat(token.getToken()).isSameAs(jwt);
    }
}
