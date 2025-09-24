# Fineract Feign Client

A Feign-based Java client for the Apache Fineract API. This module provides a type-safe, declarative HTTP client for interacting with Fineract's RESTful API.

## Features

- Type-safe API client generation using Feign
- Support for all Fineract API endpoints
- Configurable HTTP client with Apache HTTP Client 5 (hc5)
- Comprehensive error handling
- Support for authentication
- Built with Java 8+

## Installation

Add the following dependency to your project's `build.gradle`:

```groovy
dependencies {
    implementation 'org.apache.fineract:fineract-client-feign:1.0.0'
}
```

## Usage

### Basic Example

```java
// Create a client instance
FineractFeignClient client = FineractFeignClient.builder()
    .baseUrl("https://your-fineract-instance:8443/fineract-provider/api/v1")
    .credentials("username", "password")
    .build();

// Use the client to make API calls
List<ClientData> clients = client.clients().retrieveAll("default", 0, 10, "id", "ASC");
```

### Configuration Options

The `FineractFeignClient` can be configured with the following options:

```java
FineractFeignClient client = FineractFeignClient.builder()
    .baseUrl("https://your-fineract-instance:8443/fineract-provider/api/v1")
    .credentials("username", "password")
    .connectTimeout(30, TimeUnit.SECONDS)  // Connection timeout
    .readTimeout(60, TimeUnit.SECONDS)     // Read timeout
    .debug(true)                          // Enable debug logging
    .build();
```

### Available API Clients

The following API clients are available through the main `FineractFeignClient`:

- `clients()` - Client management
- `groups()` - Group management
- `loans()` - Loan account operations
- `savings()` - Savings account operations
- `selfUser()` - Current user operations

### Error Handling

The client throws `FineractClientException` for API errors, which includes:

- HTTP status code
- Error message
- Developer message (if available)
- Request details

```java
try {
    ClientData client = client.clients().retrieveOne("default", clientId);
} catch (FineractClientException e) {
    System.err.println("Error: " + e.getMessage());
    System.err.println("Status: " + e.getStatus());
}
```

## Building from Source

To build the project from source:

```bash
./gradlew :fineract-client-feign:build
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
