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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.fineract.infrastructure.core.domain.TenantOidcConfig;
import org.apache.fineract.infrastructure.security.domain.OidcFederationType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TenantOidcConfigServiceTest {

    private static final String ISSUER = "https://keycloak.example.com/realms/tenant-a";
    private static final String TENANT_ID = "tenant_a";

    @Mock
    private TenantOidcConfigRepository repository;
    @Mock
    private PasswordEncryptor passwordEncryptor;

    @InjectMocks
    private TenantOidcConfigService service;

    private TenantOidcConfig sampleConfig;

    @BeforeEach
    void setUp() {
        sampleConfig = TenantOidcConfig.builder().id(1L).tenantId(TENANT_ID).providerType(OidcFederationType.KEYCLOAK).issuerUri(ISSUER)
                .clientId("fineract-client").clientSecret("encrypted-secret").usernameClaim("preferred_username")
                .scopes("openid,profile,email").enabled(true).build();
    }

    // -------------------------------------------------------------------------
    // findByIssuerUri — cache behavior
    // -------------------------------------------------------------------------

    @Test
    void returnsConfigFromDbOnFirstCall() {
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.of(sampleConfig));

        Optional<TenantOidcConfig> result = service.findByIssuerUri(ISSUER);

        assertThat(result).isPresent();
        assertThat(result.get().getTenantId()).isEqualTo(TENANT_ID);
        verify(repository).findByIssuerUri(ISSUER);
    }

    @Test
    void returnsEmptyWhenDbHasNoRecord() {
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.empty());

        Optional<TenantOidcConfig> result = service.findByIssuerUri(ISSUER);

        assertThat(result).isEmpty();
    }

    @Test
    void cachesResultAndAvoidsDuplicateDbCalls() {
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.of(sampleConfig));

        service.findByIssuerUri(ISSUER);
        service.findByIssuerUri(ISSUER);
        service.findByIssuerUri(ISSUER);

        // DB is only called once; subsequent calls are served from cache
        verify(repository, times(1)).findByIssuerUri(ISSUER);
    }

    @Test
    void doesNotCacheMissResults() {
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.empty());

        service.findByIssuerUri(ISSUER);
        service.findByIssuerUri(ISSUER);

        // Cache only stores present values; misses always go to DB
        verify(repository, times(2)).findByIssuerUri(ISSUER);
    }

    // -------------------------------------------------------------------------
    // save — encryption + cache invalidation
    // -------------------------------------------------------------------------

    @Test
    void encryptsClientSecretBeforeSaving() {
        TenantOidcConfig newConfig = TenantOidcConfig.builder().tenantId(TENANT_ID).providerType(OidcFederationType.KEYCLOAK)
                .issuerUri(ISSUER).clientId("client").clientSecret("plain-secret").usernameClaim("preferred_username").scopes("openid")
                .enabled(true).build();

        when(passwordEncryptor.encrypt("plain-secret")).thenReturn("encrypted-secret");
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.save(newConfig);

        verify(passwordEncryptor).encrypt("plain-secret");
        verify(repository).save(any(TenantOidcConfig.class));
    }

    @Test
    void invalidatesCacheEntryOnSave() {
        // Warm the cache
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.of(sampleConfig));
        service.findByIssuerUri(ISSUER);
        verify(repository, times(1)).findByIssuerUri(ISSUER);

        // Save triggers cache eviction for that issuer
        TenantOidcConfig updated = TenantOidcConfig.builder()
                .id(1L).tenantId(TENANT_ID).providerType(OidcFederationType.KEYCLOAK)
                .issuerUri(ISSUER).clientId("client").clientSecret("new-secret")
                .usernameClaim("preferred_username").scopes("openid").enabled(true).build();
        when(passwordEncryptor.encrypt(any())).thenReturn("enc");
        when(repository.save(any())).thenReturn(updated);

        service.save(updated);

        // Next find should go to DB again (cache was evicted)
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.of(updated));
        service.findByIssuerUri(ISSUER);
        verify(repository, times(2)).findByIssuerUri(ISSUER);
    }

    @Test
    void skipsEncryptionWhenClientSecretIsBlank() {
        TenantOidcConfig publicClient = TenantOidcConfig.builder().tenantId(TENANT_ID).providerType(OidcFederationType.GOOGLE)
                .issuerUri(ISSUER).clientId("client").clientSecret(null).usernameClaim("email").scopes("openid").enabled(true).build();

        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.save(publicClient);

        verify(passwordEncryptor, never()).encrypt(any());
    }

    // -------------------------------------------------------------------------
    // decryptClientSecret
    // -------------------------------------------------------------------------

    @Test
    void decryptsClientSecretViaEncryptor() {
        when(passwordEncryptor.decrypt("encrypted-secret")).thenReturn("plain-secret");

        String result = service.decryptClientSecret(sampleConfig);

        assertThat(result).isEqualTo("plain-secret");
    }

    @Test
    void returnsNullWhenClientSecretIsNull() {
        sampleConfig.setClientSecret(null);

        String result = service.decryptClientSecret(sampleConfig);

        assertThat(result).isNull();
        verify(passwordEncryptor, never()).decrypt(any());
    }

    // -------------------------------------------------------------------------
    // deleteByTenantId — cache invalidation
    // -------------------------------------------------------------------------

    @Test
    void invalidatesCacheEntryOnDelete() {
        // Warm the cache
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.of(sampleConfig));
        service.findByIssuerUri(ISSUER);

        // Delete evicts the cache
        when(repository.findByTenantId(TENANT_ID)).thenReturn(Optional.of(sampleConfig));
        service.deleteByTenantId(TENANT_ID);

        // Next find should go to DB
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.empty());
        service.findByIssuerUri(ISSUER);

        verify(repository, times(2)).findByIssuerUri(ISSUER);
        verify(repository).deleteByTenantId(eq(TENANT_ID));
    }

    @Test
    void deleteDoesNothingWhenTenantNotFound() {
        when(repository.findByTenantId(TENANT_ID)).thenReturn(Optional.empty());

        service.deleteByTenantId(TENANT_ID);

        verify(repository, never()).deleteByTenantId(any());
    }

    // -------------------------------------------------------------------------
    // evictAll
    // -------------------------------------------------------------------------

    @Test
    void evictAllClearsCacheForAllIssuers() {
        when(repository.findByIssuerUri(ISSUER)).thenReturn(Optional.of(sampleConfig));
        service.findByIssuerUri(ISSUER);

        service.evictAll();

        // Cache is cleared — next call hits DB again
        service.findByIssuerUri(ISSUER);
        verify(repository, times(2)).findByIssuerUri(ISSUER);
    }
}
