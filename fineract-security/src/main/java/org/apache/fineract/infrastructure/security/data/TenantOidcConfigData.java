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
package org.apache.fineract.infrastructure.security.data;

import lombok.Builder;
import lombok.Getter;
import org.apache.fineract.infrastructure.core.domain.TenantOidcConfig;
import org.apache.fineract.infrastructure.security.domain.OidcFederationType;

/**
 * API response DTO for per-tenant OIDC configuration. The {@code clientSecret} field is intentionally omitted — it is
 * never returned in responses.
 */
@Builder
@Getter
public class TenantOidcConfigData {

    private String tenantId;
    private OidcFederationType providerType;
    private String issuerUri;
    private String clientId;
    private String jwksUri;
    private String usernameClaim;
    private String scopes;
    private String postLogoutRedirectUri;
    private boolean enabled;

    public static TenantOidcConfigData from(TenantOidcConfig config) {
        return TenantOidcConfigData.builder().tenantId(config.getTenantId()).providerType(config.getProviderType())
                .issuerUri(config.getIssuerUri()).clientId(config.getClientId()).jwksUri(config.getJwksUri())
                .usernameClaim(config.getUsernameClaim()).scopes(config.getScopes())
                .postLogoutRedirectUri(config.getPostLogoutRedirectUri()).enabled(config.isEnabled()).build();
    }
}
