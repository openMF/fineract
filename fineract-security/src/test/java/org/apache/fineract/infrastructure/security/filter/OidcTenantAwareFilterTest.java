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
package org.apache.fineract.infrastructure.security.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.apache.fineract.infrastructure.core.config.FineractProperties;
import org.apache.fineract.infrastructure.core.config.FineractProperties.FineractSecurityProperties.FineractSecurityOidcFederationProperties;
import org.apache.fineract.infrastructure.core.config.FineractProperties.FineractSecurityProperties.FineractSecurityOidcFederationProperties.OidcIssuerProperties;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.domain.TenantOidcConfig;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.infrastructure.security.domain.OidcFederationType;
import org.apache.fineract.infrastructure.security.service.AuthTenantDetailsService;
import org.apache.fineract.infrastructure.security.service.TenantOidcConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OidcTenantAwareFilterTest {

    private static final String TENANT_CLAIM = "fineract_tenant";
    private static final String TENANT_ID = "acme";
    private static final String ISSUER = "https://keycloak.example.com/realms/acme";

    @Mock
    private BearerTokenResolver bearerTokenResolver;
    @Mock
    private AuthTenantDetailsService tenantDetailsService;
    @Mock
    private FineractProperties fineractProperties;
    @Mock
    private FineractProperties.FineractSecurityProperties securityProperties;
    @Mock
    private FineractSecurityOidcFederationProperties oidcProps;
    @Mock
    private TenantOidcConfigService tenantOidcConfigService;
    @Mock
    private FineractPlatformTenant tenant;

    private OidcTenantAwareFilter filter;
    private MockHttpServletRequest request;
    private MockHttpServletResponse response;
    private MockFilterChain chain;

    @BeforeEach
    void setUp() {
        when(fineractProperties.getSecurity()).thenReturn(securityProperties);
        when(securityProperties.getOidcFederation()).thenReturn(oidcProps);
        when(oidcProps.getTenantClaimName()).thenReturn(TENANT_CLAIM);
        when(oidcProps.getIssuers()).thenReturn(List.of());

        filter = new OidcTenantAwareFilter(bearerTokenResolver, tenantDetailsService, fineractProperties,
                tenantOidcConfigService);
        request = new MockHttpServletRequest();
        request.setRequestURI("/api/v1/clients");
        response = new MockHttpServletResponse();
        chain = new MockFilterChain();
    }

    @AfterEach
    void tearDown() {
        ThreadLocalContextUtil.reset();
    }

    // -------------------------------------------------------------------------
    // Priority 1: DB config (iss → m_tenant_oidc_config)
    // -------------------------------------------------------------------------

    @Test
    void resolvesTenantFromDbViaIssuerClaim() throws Exception {
        String token = jwtWithIssuer(ISSUER);
        when(bearerTokenResolver.resolve(request)).thenReturn(token);

        TenantOidcConfig dbConfig = TenantOidcConfig.builder().tenantId(TENANT_ID).providerType(OidcFederationType.KEYCLOAK)
                .issuerUri(ISSUER).clientId("client").enabled(true).build();
        when(tenantOidcConfigService.findByIssuerUri(ISSUER)).thenReturn(Optional.of(dbConfig));
        when(tenantDetailsService.loadTenantById(TENANT_ID, false)).thenReturn(tenant);

        filter.doFilter(request, response, chain);

        verify(tenantDetailsService).loadTenantById(eq(TENANT_ID), eq(false));
        assertThat(chain.getRequest()).isNotNull();
    }

    @Test
    void dbResolutionTakesPriorityOverYamlAndClaim() throws Exception {
        String token = jwtWithIssuerAndCustomClaim(ISSUER, TENANT_CLAIM, "claim-based-tenant");
        when(bearerTokenResolver.resolve(request)).thenReturn(token);

        TenantOidcConfig dbConfig = TenantOidcConfig.builder().tenantId(TENANT_ID).providerType(OidcFederationType.KEYCLOAK)
                .issuerUri(ISSUER).clientId("client").enabled(true).build();
        when(tenantOidcConfigService.findByIssuerUri(ISSUER)).thenReturn(Optional.of(dbConfig));
        when(tenantDetailsService.loadTenantById(TENANT_ID, false)).thenReturn(tenant);

        filter.doFilter(request, response, chain);

        // DB-resolved tenant wins over claim-based tenant
        verify(tenantDetailsService).loadTenantById(eq(TENANT_ID), eq(false));
        verify(tenantDetailsService, never()).loadTenantById(eq("claim-based-tenant"), anyBoolean());
    }

    // -------------------------------------------------------------------------
    // Priority 2: YAML issuers[] fallback
    // -------------------------------------------------------------------------

    @Test
    void resolvesTenantFromYamlIssuerWhenDbHasNoRecord() throws Exception {
        String token = jwtWithIssuer(ISSUER);
        when(bearerTokenResolver.resolve(request)).thenReturn(token);
        when(tenantOidcConfigService.findByIssuerUri(ISSUER)).thenReturn(Optional.empty());

        OidcIssuerProperties yamlIssuer = new OidcIssuerProperties();
        yamlIssuer.setIssuerUri(ISSUER);
        yamlIssuer.setTenantId(TENANT_ID);
        when(oidcProps.getIssuers()).thenReturn(List.of(yamlIssuer));
        when(tenantDetailsService.loadTenantById(TENANT_ID, false)).thenReturn(tenant);

        filter.doFilter(request, response, chain);

        verify(tenantDetailsService).loadTenantById(eq(TENANT_ID), eq(false));
    }

    // -------------------------------------------------------------------------
    // Priority 3: custom claim (fineract_tenant) — legacy / single-realm
    // -------------------------------------------------------------------------

    @Test
    void resolvesTenantFromCustomClaimWhenDbAndYamlHaveNoRecord() throws Exception {
        String token = jwtWithIssuerAndCustomClaim(ISSUER, TENANT_CLAIM, TENANT_ID);
        when(bearerTokenResolver.resolve(request)).thenReturn(token);
        when(tenantOidcConfigService.findByIssuerUri(ISSUER)).thenReturn(Optional.empty());
        when(tenantDetailsService.loadTenantById(TENANT_ID, false)).thenReturn(tenant);

        filter.doFilter(request, response, chain);

        verify(tenantDetailsService).loadTenantById(eq(TENANT_ID), eq(false));
    }

    // -------------------------------------------------------------------------
    // Priority 4 & 5: HTTP header and query param
    // -------------------------------------------------------------------------

    @Test
    void resolvesTenantFromHttpHeader() throws Exception {
        when(bearerTokenResolver.resolve(request)).thenReturn(null);
        request.addHeader("Fineract-Platform-TenantId", TENANT_ID);
        when(tenantDetailsService.loadTenantById(TENANT_ID, false)).thenReturn(tenant);

        filter.doFilter(request, response, chain);

        verify(tenantDetailsService).loadTenantById(eq(TENANT_ID), eq(false));
    }

    @Test
    void resolvesTenantFromQueryParam() throws Exception {
        when(bearerTokenResolver.resolve(request)).thenReturn(null);
        request.addParameter("tenantIdentifier", TENANT_ID);
        when(tenantDetailsService.loadTenantById(TENANT_ID, false)).thenReturn(tenant);

        filter.doFilter(request, response, chain);

        verify(tenantDetailsService).loadTenantById(eq(TENANT_ID), eq(false));
    }

    @Test
    void headerTakesPriorityOverQueryParam() throws Exception {
        when(bearerTokenResolver.resolve(request)).thenReturn(null);
        request.addHeader("Fineract-Platform-TenantId", "from-header");
        request.addParameter("tenantIdentifier", "from-param");
        when(tenantDetailsService.loadTenantById("from-header", false)).thenReturn(tenant);

        filter.doFilter(request, response, chain);

        verify(tenantDetailsService).loadTenantById(eq("from-header"), eq(false));
        verify(tenantDetailsService, never()).loadTenantById(eq("from-param"), anyBoolean());
    }

    // -------------------------------------------------------------------------
    // Error / edge cases
    // -------------------------------------------------------------------------

    @Test
    void filterChainContinuesWhenNoTenantResolved() throws Exception {
        when(bearerTokenResolver.resolve(request)).thenReturn(null);

        filter.doFilter(request, response, chain);

        assertThat(chain.getRequest()).isNotNull();
        verify(tenantDetailsService, never()).loadTenantById(any(), anyBoolean());
    }

    @Test
    void filterChainContinuesWhenTenantServiceThrows() throws Exception {
        when(bearerTokenResolver.resolve(request)).thenReturn(null);
        request.addHeader("Fineract-Platform-TenantId", TENANT_ID);
        when(tenantDetailsService.loadTenantById(TENANT_ID, false))
                .thenThrow(new RuntimeException("DB down"));

        filter.doFilter(request, response, chain);

        assertThat(chain.getRequest()).isNotNull();
    }

    @Test
    void skipsSwaggerUiRequests() throws Exception {
        request.setRequestURI("/swagger-ui/index.html");

        filter.doFilter(request, response, chain);

        verify(tenantDetailsService, never()).loadTenantById(any(), anyBoolean());
    }

    @Test
    void skipsActuatorRequests() throws Exception {
        request.setRequestURI("/actuator/health");

        filter.doFilter(request, response, chain);

        verify(tenantDetailsService, never()).loadTenantById(any(), anyBoolean());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String jwtWithIssuer(String issuer) {
        return new PlainJWT(new JWTClaimsSet.Builder().issuer(issuer).subject("user-123")
                .expirationTime(new Date(System.currentTimeMillis() + 3600_000)).build()).serialize();
    }

    private String jwtWithIssuerAndCustomClaim(String issuer, String claimName, String claimValue) {
        return new PlainJWT(new JWTClaimsSet.Builder().issuer(issuer).subject("user-123").claim(claimName, claimValue)
                .expirationTime(new Date(System.currentTimeMillis() + 3600_000)).build()).serialize();
    }
}
