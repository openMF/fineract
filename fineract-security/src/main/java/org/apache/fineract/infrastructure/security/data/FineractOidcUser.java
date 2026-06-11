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

import java.util.Collection;
import java.util.Objects;
import org.apache.fineract.useradministration.domain.AppUser;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.OidcUserInfo;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;

/**
 * Extends {@link DefaultOidcUser} to carry the resolved Fineract {@link AppUser} and tenant ID. Used in the
 * browser-based OAuth2 login redirect flow. Bearer token API flows use {@link FineractJwtAuthenticationToken} directly
 * instead.
 */
public class FineractOidcUser extends DefaultOidcUser {

    private final AppUser appUser;
    private final String tenantId;

    public FineractOidcUser(Collection<? extends GrantedAuthority> authorities, OidcIdToken idToken, OidcUserInfo userInfo, AppUser appUser,
            String tenantId) {
        super(authorities, idToken, userInfo);
        this.appUser = Objects.requireNonNull(appUser, "appUser");
        this.tenantId = Objects.requireNonNull(tenantId, "tenantId");
    }

    public AppUser getAppUser() {
        return appUser;
    }

    public String getTenantId() {
        return tenantId;
    }
}
