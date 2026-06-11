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
package org.apache.fineract.infrastructure.core.domain;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.fineract.infrastructure.security.domain.OidcFederationType;

/**
 * Holds the per-tenant OIDC/IdP configuration stored in the master database (m_tenant_oidc_config). One record per
 * tenant; the issuerUri is the resolution key matched against the JWT 'iss' claim.
 */
@Builder
@Getter
@Setter
@EqualsAndHashCode(of = "issuerUri")
public class TenantOidcConfig {

    private Long id;
    private String tenantId;
    private OidcFederationType providerType;
    private String issuerUri;
    private String clientId;
    /** Stored AES-256-GCM encrypted; never exposed in API responses. */
    private String clientSecret;
    /** Optional: if null, derived from issuerUri via OIDC discovery. */
    private String jwksUri;
    private String usernameClaim;
    private String scopes;
    private String postLogoutRedirectUri;
    private boolean enabled;
}
