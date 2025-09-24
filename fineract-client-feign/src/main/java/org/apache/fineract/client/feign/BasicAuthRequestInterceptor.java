package org.apache.fineract.client.feign;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Request interceptor that adds Basic Authentication header to requests.
 */
public class BasicAuthRequestInterceptor implements RequestInterceptor {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BASIC_AUTH_PREFIX = "Basic ";
    
    private final String credentials;

    /**
     * Creates a new BasicAuthRequestInterceptor with the specified credentials.
     *
     * @param username the username for authentication
     * @param password the password for authentication
     */
    public BasicAuthRequestInterceptor(String username, String password) {
        if (username == null || password == null) {
            throw new IllegalArgumentException("Username and password cannot be null");
        }
        String auth = username + ":" + password;
        this.credentials = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void apply(RequestTemplate template) {
        template.header(AUTHORIZATION_HEADER, BASIC_AUTH_PREFIX + credentials);
    }
}
