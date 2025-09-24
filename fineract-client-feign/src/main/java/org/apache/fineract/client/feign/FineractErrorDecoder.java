package org.apache.fineract.client.feign;

import com.fasterxml.jackson.databind.ObjectMapper;
import feign.Response;
import feign.codec.ErrorDecoder;
import org.apache.fineract.client.feign.model.Error;
import org.apache.fineract.client.feign.model.ErrorResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Decodes error responses from the Fineract API into appropriate exceptions.
 */
public class FineractErrorDecoder implements ErrorDecoder {

    private final ObjectMapper objectMapper;
    private final ErrorDecoder defaultDecoder = new Default();

    public FineractErrorDecoder() {
        this.objectMapper = ObjectMapperFactory.getShared();
    }

    @Override
    public Exception decode(String methodKey, Response response) {
        try {
            // First try to parse the error response as a Fineract error
            if (response.body() != null) {
                ErrorResponse errorResponse = objectMapper.readValue(
                    response.body().asInputStream(),
                    ErrorResponse.class
                );
                
                if (errorResponse != null && errorResponse.getErrors() != null && !errorResponse.getErrors().isEmpty()) {
                    Error error = errorResponse.getErrors().get(0);
                    return new FineractClientException(
                        response.status(),
                        error.getDefaultUserMessage(),
                        error.getDeveloperMessage(),
                        error.getUserMessageGlobalisationCode(),
                        error.getParameterName(),
                        error.getValue(),
                        response.request()
                    );
                }
            }
            
            // Fall back to the default error decoder
            return defaultDecoder.decode(methodKey, response);
            
        } catch (IOException e) {
            return new FineractClientException(
                response.status(),
                "Error processing error response: " + e.getMessage(),
                e,
                response.request()
            );
        }
    }

    /**
     * Custom exception for Fineract API errors.
     */
    public static class FineractClientException extends FeignException {
        private final int status;
        private final String developerMessage;
        private final String userMessageGlobalisationCode;
        private final String parameterName;
        private final Object value;

        public FineractClientException(int status, String message, 
                                     String developerMessage, 
                                     String userMessageGlobalisationCode,
                                     String parameterName, 
                                     Object value,
                                     Request request) {
            super(status, message, request);
            this.status = status;
            this.developerMessage = developerMessage;
            this.userMessageGlobalisationCode = userMessageGlobalisationCode;
            this.parameterName = parameterName;
            this.value = value;
        }

        public FineractClientException(int status, String message, Throwable cause, Request request) {
            super(status, message, request, cause);
            this.status = status;
            this.developerMessage = message;
            this.userMessageGlobalisationCode = null;
            this.parameterName = null;
            this.value = null;
        }

        public int getStatus() {
            return status;
        }

        public String getDeveloperMessage() {
            return developerMessage;
        }

        public String getUserMessageGlobalisationCode() {
            return userMessageGlobalisationCode;
        }

        public String getParameterName() {
            return parameterName;
        }

        public Object getValue() {
            return value;
        }
    }
}
