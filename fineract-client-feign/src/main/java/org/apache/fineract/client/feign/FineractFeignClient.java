package org.apache.fineract.client.feign;

import org.apache.fineract.client.feign.api.*;
import org.apache.fineract.client.services.ClientApi;
import org.apache.fineract.client.services.LoansApi;

/**
 * Main entry point for creating Feign-based clients for the Fineract API.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 * FineractFeignClient client = FineractFeignClient.builder()
 *     .baseUrl("https://localhost:8443/fineract-provider/api/v1")
 *     .credentials("username", "password")
 *     .build();
 *
 * // Access API clients
 * ClientsApiClient clientsApi = client.clients();
 * List<ClientData> clients = clientsApi.retrieveAll();
 * }
 * </pre>
 */
public class FineractFeignClient {

    private final FineractFeignClientConfig config;

    private FineractFeignClient(Builder builder) {
        this.config = builder.configBuilder.build();
    }

    /**
     * Creates a new builder for configuring a FineractFeignClient.
     *
     * @return A new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a new client for the specified API interface.
     *
     * @param <T> The API interface type
     * @param apiType The API interface class
     * @return A configured Feign client for the specified API
     */
    public <T> T create(Class<T> apiType) {
        return config.createClient(apiType);
    }

    // Convenience methods for accessing API clients

    public ClientApi clients() {
        return create(ClientApi.class);
    }

    public LoansApi loans() {
        return create(LoansApi.class);
    }

    /**
     * Builder for creating and configuring a FineractFeignClient.
     */
    public static class Builder {
        private final FineractFeignClientConfig.Builder configBuilder = FineractFeignClientConfig.builder();

        /**
         * Sets the base URL for the Fineract API.
         *
         * @param baseUrl The base URL (e.g., "https://localhost:8443/fineract-provider/api/v1")
         * @return This builder instance
         */
        public Builder baseUrl(String baseUrl) {
            configBuilder.baseUrl(baseUrl);
            return this;
        }

        /**
         * Sets the credentials for Basic Authentication.
         *
         * @param username The username
         * @param password The password
         * @return This builder instance
         */
        public Builder credentials(String username, String password) {
            configBuilder.credentials(username, password);
            return this;
        }

        /**
         * Sets the connection timeout.
         *
         * @param timeout The timeout value
         * @param unit The time unit
         * @return This builder instance
         */
        public Builder connectTimeout(long timeout, java.util.concurrent.TimeUnit unit) {
            configBuilder.connectTimeout(timeout, unit);
            return this;
        }

        /**
         * Sets the read timeout.
         *
         * @param timeout The timeout value
         * @param unit The time unit
         * @return This builder instance
         */
        public Builder readTimeout(long timeout, java.util.concurrent.TimeUnit unit) {
            configBuilder.readTimeout(timeout, unit);
            return this;
        }

        /**
         * Enables or disables debug logging.
         *
         * @param enabled true to enable debug logging, false to disable
         * @return This builder instance
         */
        public Builder debug(boolean enabled) {
            configBuilder.debugEnabled(enabled);
            return this;
        }

        /**
         * Builds a new FineractFeignClient with the current configuration.
         *
         * @return A new FineractFeignClient instance
         */
        public FineractFeignClient build() {
            return new FineractFeignClient(this);
        }
    }
}
