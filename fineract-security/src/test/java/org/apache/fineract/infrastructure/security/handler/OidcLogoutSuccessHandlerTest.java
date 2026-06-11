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
package org.apache.fineract.infrastructure.security.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.fineract.infrastructure.core.config.FineractProperties;
import org.apache.fineract.infrastructure.security.data.FineractOidcUser;
import org.apache.fineract.infrastructure.security.domain.OidcFederationType;
import org.apache.fineract.useradministration.domain.AppUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OidcLogoutSuccessHandlerTest {

    private static final String ISSUER = "https://idp.example.com/realm/fineract";
    private static final String POST_LOGOUT_URI = "https://app.example.com/login?logout";

    @Mock
    private FineractProperties fineractProperties;
    @Mock
    private FineractProperties.FineractSecurityProperties securityProperties;
    @Mock
    private FineractProperties.FineractSecurityProperties.FineractSecurityOidcFederationProperties oidcProps;
    @Mock
    private AppUser appUser;

    private OidcLogoutSuccessHandler handler;
    private MockHttpServletRequest request;
    private MockHttpServletResponse response;

    @BeforeEach
    void setUp() {
        when(fineractProperties.getSecurity()).thenReturn(securityProperties);
        when(securityProperties.getOidcFederation()).thenReturn(oidcProps);
        when(oidcProps.getPostLogoutRedirectUri()).thenReturn(POST_LOGOUT_URI);
        handler = new OidcLogoutSuccessHandler(fineractProperties);
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
    }

    private Authentication authForProvider(OidcFederationType provider) {
        when(oidcProps.getProvider()).thenReturn(provider);
        OidcIdToken idToken = new OidcIdToken("raw-id-token",
                Instant.now(), Instant.now().plusSeconds(3600),
                Map.of("sub", "user-123", "iss", ISSUER));
        FineractOidcUser oidcUser = new FineractOidcUser(List.of(), idToken, null, appUser, "acme");
        Authentication auth = mock(Authentication.class);
        when(auth.getPrincipal()).thenReturn(oidcUser);
        return auth;
    }

    @Test
    void keycloakLogoutUrlContainsExpectedPathAndParams() throws Exception {
        handler.onLogoutSuccess(request, response, authForProvider(OidcFederationType.KEYCLOAK));

        String url = response.getRedirectedUrl();
        assertThat(url).contains("/protocol/openid-connect/logout").contains("id_token_hint=raw-id-token")
                .contains("post_logout_redirect_uri=");
    }

    @Test
    void azureAdLogoutUrlContainsExpectedPathAndParams() throws Exception {
        handler.onLogoutSuccess(request, response, authForProvider(OidcFederationType.AZURE));

        String url = response.getRedirectedUrl();
        assertThat(url).contains("/oauth2/v2.0/logout").contains("post_logout_redirect_uri=");
    }

    @Test
    void oktaLogoutUrlContainsExpectedPathAndParams() throws Exception {
        handler.onLogoutSuccess(request, response, authForProvider(OidcFederationType.OKTA));

        String url = response.getRedirectedUrl();
        assertThat(url).contains("/v1/logout").contains("id_token_hint=raw-id-token").contains("post_logout_redirect_uri=");
    }

    @Test
    void auth0LogoutUrlContainsReturnToParam() throws Exception {
        handler.onLogoutSuccess(request, response, authForProvider(OidcFederationType.AUTH0));

        String url = response.getRedirectedUrl();
        assertThat(url).contains("/v2/logout").contains("returnTo=");
    }

    @Test
    void genericProviderFallsBackToLocalLogoutPage() throws Exception {
        handler.onLogoutSuccess(request, response, authForProvider(OidcFederationType.GENERIC));

        assertThat(response.getRedirectedUrl()).contains("/login?logout");
    }

    @Test
    void nullAuthenticationFallsBackToLocalLogoutPage() throws Exception {
        handler.onLogoutSuccess(request, response, null);

        assertThat(response.getRedirectedUrl()).contains("/login?logout");
    }

    @Test
    void postLogoutUriBuiltFromRequestHostWhenPropertyBlank() throws Exception {
        when(oidcProps.getPostLogoutRedirectUri()).thenReturn(null);
        when(oidcProps.getProvider()).thenReturn(OidcFederationType.OKTA);

        request.setScheme("https");
        request.setServerName("fineract.example.com");
        request.setServerPort(443);

        OidcIdToken idToken = new OidcIdToken("tok",
                Instant.now(), Instant.now().plusSeconds(3600),
                Map.of("sub", "u", "iss", ISSUER));
        FineractOidcUser oidcUser = new FineractOidcUser(List.of(), idToken, null, appUser, "acme");
        Authentication auth = mock(Authentication.class);
        when(auth.getPrincipal()).thenReturn(oidcUser);

        handler.onLogoutSuccess(request, response, auth);

        assertThat(response.getRedirectedUrl()).contains("fineract.example.com");
    }
}
