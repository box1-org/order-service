package org.box1.order;

import static io.restassured.RestAssured.given;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.response.Response;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Test helpers that talk to the real database and broker. */
final class Harness {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String ORDER_TOPIC = "box1.order.events.v1";
    static final String INVENTORY_TOPIC = "box1.inventory.events.v1";
    static final String FULFILLMENT_TOPIC = "box1.fulfillment.events.v1";
    static final String DLQ_TOPIC = "box1.order.dlq.v1";

    private Harness() {}

    private static boolean brokerPaused;

    static void pauseBroker() {
        brokerPaused = true;
        retrying(() -> FullStackResource.kafka.getDockerClient()
                .pauseContainerCmd(FullStackResource.kafka.getContainerId()).exec());
    }

    static void resumeBroker() {
        if (!brokerPaused) {
            return;
        }
        brokerPaused = false;
        retrying(() -> FullStackResource.kafka.getDockerClient()
                .unpauseContainerCmd(FullStackResource.kafka.getContainerId()).exec());
    }

    /** The container client can hold a stale pooled connection after idling, so these commands retry. */
    private static void retrying(Runnable command) {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                command.run();
                return;
            } catch (RuntimeException error) {
                last = error;
            }
        }
        throw last;
    }

    static String body(String customer) {
        return "{\"customerId\":\"" + customer + "\",\"sku\":\"SKU-42\",\"quantity\":2,\"siteId\":\"site_nyc\"}";
    }

    static Response post(String key, String body) {
        return given().header("Idempotency-Key", key).contentType("application/json").body(body)
                .when().post("/api/v1/orders");
    }

    static String newOrder() {
        return post(UUID.randomUUID().toString(), body("cust_demo_1")).then().statusCode(201).extract().path("orderId");
    }

    static Connection db() throws SQLException {
        return DriverManager.getConnection(FullStackResource.postgres.getJdbcUrl(),
                FullStackResource.postgres.getUsername(), FullStackResource.postgres.getPassword());
    }

    static long count(String sql, Object... args) {
        try (Connection c = db(); PreparedStatement s = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                s.setObject(i + 1, args[i]);
            }
            try (ResultSet r = s.executeQuery()) {
                r.next();
                return r.getLong(1);
            }
        } catch (SQLException error) {
            throw new IllegalStateException(error);
        }
    }

    static void exec(String sql) {
        try (Connection c = db(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException error) {
            throw new IllegalStateException(error);
        }
    }

    static String state(String orderId) {
        return given().when().get("/api/v1/orders/" + orderId).then().statusCode(200).extract().path("state");
    }

    static void await(String what, BooleanSupplier condition) {
        await(what, Duration.ofSeconds(60), condition);
    }

    static void await(String what, Duration limit, BooleanSupplier condition) {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(error);
            }
        }
        throw new AssertionError("Timed out waiting for " + what);
    }

    static void send(String topic, String key, String value) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FullStackResource.kafka.getBootstrapServers());
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    /** Read everything currently on a topic with a throwaway consumer group. */
    static List<ConsumerRecord<String, String>> read(String topic, Duration settle) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, FullStackResource.kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + settle.toNanos();
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(records::add);
            }
        }
        return records;
    }

    /** Events on the order topic for one order. */
    static List<JsonNode> orderEvents(String orderId, Duration settle) {
        List<JsonNode> events = new ArrayList<>();
        for (ConsumerRecord<String, String> record : read(ORDER_TOPIC, settle)) {
            if (orderId.equals(record.key())) {
                try {
                    events.add(JSON.readTree(record.value()));
                } catch (com.fasterxml.jackson.core.JsonProcessingException error) {
                    throw new IllegalStateException(error);
                }
            }
        }
        return events;
    }

    static String envelope(String eventId, String type, String orderId, Map<String, Object> payload) {
        Map<String, Object> full = new java.util.LinkedHashMap<>(payload);
        full.put("orderId", orderId);
        Map<String, Object> envelope = new java.util.LinkedHashMap<>();
        envelope.put("eventId", eventId);
        envelope.put("eventType", type);
        envelope.put("schemaVersion", 1);
        envelope.put("aggregateId", orderId);
        envelope.put("occurredAt", "2026-11-05T12:00:00Z");
        envelope.put("correlationId", "corr-test");
        envelope.put("causationId", "cause-test");
        envelope.put("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        envelope.put("payload", full);
        try {
            return JSON.writeValueAsString(envelope);
        } catch (com.fasterxml.jackson.core.JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }

    static String reserved(String eventId, String orderId) {
        return envelope(eventId, "inventory.reserved.v1", orderId, Map.of("reservationId", "res_" + orderId.substring(4),
                "sku", "SKU-42", "quantity", 2, "siteId", "site_nyc", "expiresAt", "2099-01-01T00:00:00Z"));
    }

    static String rejected(String eventId, String orderId) {
        return envelope(eventId, "inventory.rejected.v1", orderId, Map.of("sku", "SKU-42", "quantity", 2,
                "siteId", "site_nyc", "reasonCode", "INSUFFICIENT_STOCK"));
    }

    static String shipped(String eventId, String orderId) {
        return envelope(eventId, "shipment.confirmed.v1", orderId, Map.of("fulfillmentRequestId", "ful_" + orderId.substring(4),
                "shipmentId", "shp_1", "shippedAt", "2026-11-05T14:00:00Z"));
    }

    static String id() {
        return UUID.randomUUID().toString();
    }
}
