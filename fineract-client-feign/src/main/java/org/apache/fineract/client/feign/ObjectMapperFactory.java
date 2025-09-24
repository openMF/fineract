package org.apache.fineract.client.feign;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Factory for creating and configuring Jackson ObjectMapper instances.
 */
public class ObjectMapperFactory {

    private static final ObjectMapper INSTANCE = createObjectMapper();

    private ObjectMapperFactory() {
        // Private constructor to prevent instantiation
    }

    /**
     * Creates and configures a new ObjectMapper instance.
     * 
     * @return A new configured ObjectMapper instance
     */
    public static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        
        // Configure the ObjectMapper
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        
        // Register Java 8 date/time support
        mapper.registerModule(new JavaTimeModule());
        
        // Disable FAIL_ON_EMPTY_BEANS for empty responses
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        
        return mapper;
    }

    /**
     * Returns a shared, pre-configured ObjectMapper instance.
     * 
     * @return A shared ObjectMapper instance
     */
    public static ObjectMapper getShared() {
        return INSTANCE;
    }
}
