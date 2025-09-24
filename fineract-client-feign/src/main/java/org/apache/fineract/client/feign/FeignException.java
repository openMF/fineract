package org.apache.fineract.client.feign;

import feign.Request;

import java.nio.charset.Charset;

/**
 * Base exception class for Feign client exceptions.
 */
public class FeignException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int status;
    private final Request request;
    private final byte[] responseBody;

    protected FeignException(int status, String message, Request request) {
        this(status, message, request, (byte[]) null);
    }

    protected FeignException(int status, String message, Request request, Throwable cause) {
        this(status, message, request, null, cause);
    }

    protected FeignException(int status, String message, Request request, byte[] responseBody) {
        super(message);
        this.status = status;
        this.request = request;
        this.responseBody = responseBody;
    }

    protected FeignException(int status, String message, Request request, byte[] responseBody, Throwable cause) {
        super(message, cause);
        this.status = status;
        this.request = request;
        this.responseBody = responseBody;
    }

    public int status() {
        return status;
    }

    public Request request() {
        return request;
    }

    public byte[] responseBody() {
        return responseBody;
    }

    public String responseBodyAsString() {
        return responseBody != null ? new String(responseBody, Charset.defaultCharset()) : null;
    }
}
