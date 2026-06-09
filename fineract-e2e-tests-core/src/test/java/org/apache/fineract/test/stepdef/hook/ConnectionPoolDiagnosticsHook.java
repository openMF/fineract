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
package org.apache.fineract.test.stepdef.hook;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.Scenario;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.test.api.ApiProperties;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class ConnectionPoolDiagnosticsHook {

    private static final Pattern PROMETHEUS_SAMPLE = Pattern
            .compile("^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\\{[^}]*})?\\s+([+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?)$");
    private static final String TENANT_PREFIX = "fineract_tenants_";
    private static final String TENANT_HIKARI = "_hikaricp_connections";
    private static final String MASTER_HIKARI = "hikaricp_connections";
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);

    @Autowired
    private ApiProperties apiProperties;

    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final AtomicReference<ConnectionPoolMetrics> latest = new AtomicReference<>();
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> samplingTask;
    private ConnectionPoolMetrics baseline;
    private ConnectionPoolPeaks peaks;

    @Before(value = "@ConnectionPoolDiagnostics", order = 1000)
    public void startConnectionPoolDiagnostics(Scenario scenario) {
        baseline = fetchMetrics();
        // Fail fast if Prometheus is not reachable or the tenant pool metrics are absent.
        // This hook is opt-in via the @ConnectionPoolDiagnostics tag, so it should only
        // be applied to scenarios running against a fully provisioned environment where
        // the actuator endpoint is guaranteed to be available.
        assertThat(baseline.hasTenantMetrics()).as("No tenant Hikari metrics were found at %s", prometheusUri()).isTrue();

        peaks = new ConnectionPoolPeaks(baseline);
        latest.set(baseline);
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "connection-pool-diagnostics");
            thread.setDaemon(true);
            return thread;
        });
        samplingTask = executor.scheduleAtFixedRate(this::sampleMetrics, 0, POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);

        logSnapshot(scenario, "Connection pool diagnostics baseline", baseline);
    }

    @After(value = "@ConnectionPoolDiagnostics", order = 1000)
    public void stopConnectionPoolDiagnostics(Scenario scenario) {
        if (samplingTask != null) {
            samplingTask.cancel(true);
        }
        if (executor != null) {
            executor.shutdownNow();
        }

        int recoveryTimeoutSeconds = Integer.getInteger("fineract-test.connection-pool.recovery-timeout-seconds", 30);
        await().atMost(Duration.ofSeconds(recoveryTimeoutSeconds)).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
            ConnectionPoolMetrics current = fetchMetrics();
            latest.set(current);
            assertThat(current.tenantPending()).as("Tenant Hikari pending connections after scenario").isZero();
            assertThat(current.masterPending()).as("Master Hikari pending connections after scenario").isZero();
            assertThat(current.tenantActive()).as("Tenant Hikari active connections after scenario")
                    .isLessThanOrEqualTo(baseline.tenantActive());
        });

        ConnectionPoolMetrics current = latest.get();
        logSnapshot(scenario, "Connection pool diagnostics final", current);
        scenario.log("Connection pool diagnostics peaks: " + peaks);

        assertThat(current.tenantTimeouts()).as("Tenant Hikari connection timeouts increased").isEqualTo(baseline.tenantTimeouts());
        assertThat(current.masterTimeouts()).as("Master Hikari connection timeouts increased").isEqualTo(baseline.masterTimeouts());
    }

    private void sampleMetrics() {
        try {
            ConnectionPoolMetrics metrics = fetchMetrics();
            latest.set(metrics);
            peaks.record(metrics);
        } catch (Exception e) {
            log.warn("Unable to sample Hikari connection pool metrics", e);
        }
    }

    private ConnectionPoolMetrics fetchMetrics() {
        URI uri = prometheusUri();
        if ("https".equalsIgnoreCase(uri.getScheme())) {
            return ConnectionPoolMetrics.parse(fetchMetricsWithCurl(uri));
        }

        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).as("Prometheus actuator endpoint status").isEqualTo(200);
            return ConnectionPoolMetrics.parse(response.body());
        } catch (IOException e) {
            throw new AssertionError("Unable to fetch Prometheus metrics from " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while fetching Prometheus metrics from " + uri, e);
        }
    }

    private String fetchMetricsWithCurl(URI uri) {
        // -k disables certificate verification; acceptable here because test environments
        // typically use self-signed certificates on the HTTPS endpoint.
        ProcessBuilder processBuilder = new ProcessBuilder("curl", "-k", "--fail", "--silent", "--show-error", "--max-time", "10",
                uri.toString());
        try {
            Process process = processBuilder.start();
            String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            int exitCode = process.waitFor();
            assertThat(exitCode).as("Prometheus actuator curl exit code. stderr: %s", stderr).isZero();
            return stdout;
        } catch (IOException e) {
            throw new AssertionError("Unable to fetch Prometheus metrics from " + uri + " using curl", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while fetching Prometheus metrics from " + uri + " using curl", e);
        }
    }

    private URI prometheusUri() {
        String baseUrl = apiProperties.getBaseUrl();
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        return URI.create(baseUrl + "/fineract-provider/actuator/prometheus");
    }

    private void logSnapshot(Scenario scenario, String label, ConnectionPoolMetrics metrics) {
        String message = "%s: tenant(active=%s, pending=%s, timeouts=%s, total=%s, idle=%s, max=%s), master(active=%s, pending=%s, timeouts=%s, total=%s, idle=%s, max=%s)"
                .formatted(label, metrics.tenantActive(), metrics.tenantPending(), metrics.tenantTimeouts(), metrics.tenantTotal(),
                        metrics.tenantIdle(), metrics.tenantMax(), metrics.masterActive(), metrics.masterPending(),
                        metrics.masterTimeouts(), metrics.masterTotal(), metrics.masterIdle(), metrics.masterMax());
        scenario.log(message);
        log.info(message);
    }

    @RequiredArgsConstructor
    private static final class ConnectionPoolMetrics {

        private final Map<String, Double> metrics;

        static ConnectionPoolMetrics parse(String prometheusBody) {
            Map<String, Double> metrics = new HashMap<>();
            prometheusBody.lines().filter(line -> !line.isBlank() && !line.startsWith("#")).forEach(line -> {
                Matcher matcher = PROMETHEUS_SAMPLE.matcher(line);
                if (matcher.matches()) {
                    // Labels are stripped from the key; merge by summing so that multiple samples
                    // for the same metric name with different label sets (e.g. multiple Hikari
                    // pools sharing a metric name) are accumulated rather than overwritten.
                    metrics.merge(matcher.group(1), Double.parseDouble(matcher.group(2)), Double::sum);
                }
            });
            return new ConnectionPoolMetrics(metrics);
        }

        boolean hasTenantMetrics() {
            return metrics.keySet().stream().anyMatch(name -> name.startsWith(TENANT_PREFIX) && name.contains(TENANT_HIKARI));
        }

        long tenantActive() {
            return tenantSum("_active");
        }

        long tenantPending() {
            return tenantSum("_pending");
        }

        long tenantTimeouts() {
            return tenantSum("_timeout_total");
        }

        long tenantTotal() {
            return tenantSum("");
        }

        long tenantIdle() {
            return tenantSum("_idle");
        }

        long tenantMax() {
            return tenantSum("_max");
        }

        long masterActive() {
            return metric(MASTER_HIKARI + "_active");
        }

        long masterPending() {
            return metric(MASTER_HIKARI + "_pending");
        }

        long masterTimeouts() {
            return metric(MASTER_HIKARI + "_timeout_total");
        }

        long masterTotal() {
            return metric(MASTER_HIKARI);
        }

        long masterIdle() {
            return metric(MASTER_HIKARI + "_idle");
        }

        long masterMax() {
            return metric(MASTER_HIKARI + "_max");
        }

        private long tenantSum(String suffix) {
            return Math.round(metrics.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(TENANT_PREFIX) && entry.getKey().endsWith(TENANT_HIKARI + suffix))
                    .mapToDouble(Map.Entry::getValue).sum());
        }

        private long metric(String name) {
            return Math.round(metrics.getOrDefault(name, 0D));
        }
    }

    @Getter
    @ToString
    private static final class ConnectionPoolPeaks {

        private long maxTenantActive;
        private long maxTenantPending;
        private long maxMasterActive;
        private long maxMasterPending;

        private ConnectionPoolPeaks(ConnectionPoolMetrics baseline) {
            record(baseline);
        }

        private void record(ConnectionPoolMetrics metrics) {
            maxTenantActive = Math.max(maxTenantActive, metrics.tenantActive());
            maxTenantPending = Math.max(maxTenantPending, metrics.tenantPending());
            maxMasterActive = Math.max(maxMasterActive, metrics.masterActive());
            maxMasterPending = Math.max(maxMasterPending, metrics.masterPending());
        }
    }
}
