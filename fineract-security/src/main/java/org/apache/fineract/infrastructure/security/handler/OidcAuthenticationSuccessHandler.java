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

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.infrastructure.security.data.FineractOidcUser;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;

/**
 * Handles successful browser-based OIDC login (OAuth2 authorization code redirect flow).
 *
 * <p>
 * Logs contextual information about the authenticated user and then delegates to
 * {@link SavedRequestAwareAuthenticationSuccessHandler} so the browser is redirected back to the originally requested
 * URL.
 *
 * <p>
 * This handler is only relevant for the interactive login redirect flow. Bearer token API requests do not trigger it.
 */
@Slf4j
public class OidcAuthenticationSuccessHandler extends SavedRequestAwareAuthenticationSuccessHandler {

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication)
            throws IOException, ServletException {

        if (authentication.getPrincipal() instanceof FineractOidcUser oidcUser) {
            log.debug("OIDC login successful — user: '{}', tenant: '{}', Fineract id: {}", oidcUser.getName(), oidcUser.getTenantId(),
                    oidcUser.getAppUser() != null ? oidcUser.getAppUser().getId() : "unknown");
        }

        super.onAuthenticationSuccess(request, response, authentication);
    }
}
