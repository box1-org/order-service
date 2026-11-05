package org.box1.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Kills the packaged application with SIGKILL during real work, then restarts it with the prod profile. */
@Tag("containers")
class ProcessRestartIT {
    /** After SIGKILL the broker reassigns partitions only when the dead member's 45 s session expires. */
    private static final Duration AFTER_KILL = Duration.ofSeconds(150);
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static PostgreSQLContainer<?> postgres;
    private static KafkaContainer kafka;

    @BeforeAll
    static void containers() {
        FullStackResource resource = new FullStackResource();
        resource.start();
        postgres = FullStackResource.postgres;
        kafka = FullStackResource.kafka;
    }

    @AfterAll
    static void stop() {
        kafka.stop();
        postgres.stop();
    }

    private record App(Process process, int port, Path log) {
        void kill() throws InterruptedException {
            process.destroyForcibly();
            process.waitFor();
        }
    }

    private static App start() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Path log = Files.createTempFile("order-service", ".log");
        ProcessBuilder builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", Path.of("target", "quarkus-app", "quarkus-run.jar").toString());
        builder.environment().put("QUARKUS_HTTP_PORT", Integer.toString(port));
        builder.environment().put("QUARKUS_DATASOURCE_JDBC_URL", postgres.getJdbcUrl());
        builder.environment().put("QUARKUS_DATASOURCE_USERNAME", postgres.getUsername());
        builder.environment().put("QUARKUS_DATASOURCE_PASSWORD", postgres.getPassword());
        builder.environment().put("KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
        builder.environment().put("ORDER_OUTBOX_POLL_INTERVAL", "200ms");
        builder.environment().put("ORDER_OUTBOX_LEASE_SECONDS", "5");
        builder.environment().put("ORDER_METRICS_REFRESH_INTERVAL", "1s");
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        App app = new App(builder.start(), port, log);
        Harness.await("application ready", () -> {
            try {
                return get(app, "/q/health/ready").statusCode() == 200;
            } catch (Exception error) {
                return false;
            }
        });
        return app;
    }

    private static HttpResponse<String> get(App app, String path) throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static String create(App app) throws Exception {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + "/api/v1/orders"))
                .header("Idempotency-Key", UUID.randomUUID().toString()).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(Harness.body("cust_restart"))).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());
        return Harness.JSON.readTree(response.body()).get("orderId").asText();
    }

    @Test
    void killDuringMessageProcessingRedeliversAndAppliesOnce() throws Exception {
        App first = start();
        String orderId = create(first);
        String eventId = UUID.randomUUID().toString();
        Harness.exec("CREATE OR REPLACE FUNCTION hang_processed() RETURNS trigger AS $$ BEGIN"
                + " IF NEW.event_id = '" + eventId + "' THEN PERFORM pg_sleep(600); END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        Harness.exec("CREATE TRIGGER hang_processed_trigger BEFORE INSERT ON processed_event FOR EACH ROW EXECUTE FUNCTION hang_processed()");
        Harness.send(Harness.INVENTORY_TOPIC, orderId, Harness.reserved(eventId, orderId));
        Harness.await("the transaction to hang inside the trigger", AFTER_KILL, () -> Harness.count(
                "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'INSERT INTO processed_event%' AND wait_event = 'PgSleep'") == 1);
        first.kill();
        Harness.exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event = 'PgSleep'");
        Harness.exec("DROP TRIGGER hang_processed_trigger ON processed_event");
        Harness.await("uncommitted effects gone", () -> Harness.count("SELECT count(*) FROM processed_event WHERE event_id = ?::uuid", eventId) == 0);

        App second = start();
        try {
            Harness.await("redelivery after restart", AFTER_KILL, () -> Harness.count("SELECT count(*) FROM orders WHERE order_id = ? AND state = 'FULFILLMENT_REQUESTED'", orderId) == 1);
            assertEquals(1, Harness.count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = 'fulfillment.requested.v1'", orderId));
            assertEquals(3, Harness.count("SELECT count(*) FROM order_state_history WHERE order_id = ? AND kind = 'TRANSITION'", orderId));
        } finally {
            second.kill();
        }
    }

    @Test
    void killWhileClaimingPublishesAfterLeaseExpiry() throws Exception {
        App first = start();
        Harness.pauseBroker();
        App second = null;
        try {
            String orderId = create(first);
            for (int i = 0; i < 4; i++) {
                create(first);
            }
            Harness.await("an active claim", () -> Harness.count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND claimed_by IS NOT NULL", orderId) == 1);
            first.kill();
            assertEquals(1, Harness.count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NULL", orderId));
            // A cold start with no broker metadata must keep failing fast rather than outlast its lease.
            second = start();
            App running = second;
            Harness.await("a failed attempt from the restarted publisher", () -> {
                try {
                    return Files.readString(running.log()).contains("Outbox publish not confirmed");
                } catch (IOException error) {
                    return false;
                }
            });
            Harness.resumeBroker();
            Harness.await("publish after restart", AFTER_KILL, () -> Harness.count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", orderId) == 1);
            List<JsonNode> events = Harness.orderEvents(orderId, Duration.ofSeconds(3));
            assertTrue(events.size() >= 1);
            assertEquals(1, events.stream().map(e -> e.get("eventId").asText()).distinct().count(), "one logical event");
        } finally {
            Harness.resumeBroker();
            if (second != null) {
                second.kill();
            }
        }
    }

    @Test
    void prodProfileLogsAreStructuredAndFreeOfCustomerData() throws Exception {
        App app = start();
        String orderId = create(app);
        Harness.await("order log line", () -> {
            try {
                return Files.readString(app.log()).contains(orderId);
            } catch (IOException error) {
                return false;
            }
        });
        app.kill();
        List<String> lines = Files.readAllLines(app.log());
        String orderLine = lines.stream().filter(l -> l.contains("Order created")).findFirst().orElseThrow();
        JsonNode json = Harness.JSON.readTree(orderLine);
        assertEquals(orderId, json.path("mdc").path("orderId").asText());
        assertEquals(32, json.path("mdc").path("traceId").asText().length(), "trace ID in the log line");
        assertTrue(lines.stream().noneMatch(l -> l.contains("cust_restart")), "no customer ID in any log line");
    }
}
