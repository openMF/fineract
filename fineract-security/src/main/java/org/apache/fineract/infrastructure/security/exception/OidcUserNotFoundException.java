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
package org.apache.fineract.infrastructure.security.exception;

import org.springframework.security.core.AuthenticationException;

/**
 * Thrown when an OIDC-authenticated principal cannot be mapped to a Fineract AppUser and auto-creation is disabled.
 */
public class OidcUserNotFoundException extends AuthenticationException {

    private final String subject;

    public OidcUserNotFoundException(String subject) {
        super("No Fineract user found for OIDC subject: " + subject
                + ". Ensure the user exists in Fineract or enable fineract.security.oidc-federation.auto-create-user=true");
        this.subject = subject;
    }

    public String getSubject() {
        return subject;
    }
}
