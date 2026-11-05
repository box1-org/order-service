package org.box1.order;

import static io.restassured.RestAssured.given;
import static org.box1.order.Harness.FULFILLMENT_TOPIC;
import static org.box1.order.Harness.INVENTORY_TOPIC;
import static org.box1.order.Harness.count;
import static org.box1.order.Harness.exec;
import static org.box1.order.Harness.id;
import static org.box1.order.Harness.newOrder;
import static org.box1.order.Harness.rejected;
import static org.box1.order.Harness.reserved;
import static org.box1.order.Harness.send;
import static org.box1.order.Harness.shipped;
import static org.box1.order.Harness.state;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ContainersTag
class SagaScenariosTest {
    private static final String FULFILLMENT = "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = 'fulfillment.requested.v1'";
    private static final String NOTES = "SELECT count(*) FROM order_state_history WHERE order_id = ? AND kind = 'NOTE'";

    private static void awaitState(String orderId, String expected) {
        Harness.await(expected, () -> expected.equals(state(orderId)));
    }

    private static void awaitProcessed(String eventId) {
        Harness.await("event processed", () -> count("SELECT count(*) FROM processed_event WHERE event_id = ?::uuid", eventId) == 1);
    }

    private static List<String> reasons(String orderId) {
        return given().when().get("/api/v1/orders/" + orderId + "/timeline").then().statusCode(200).extract().path("reasonCode");
    }

    private static List<ConsumerRecord<String, String>> dlq(String key) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        for (ConsumerRecord<String, String> record : Harness.read(Harness.DLQ_TOPIC, Duration.ofSeconds(3))) {
            if (key.equals(record.key())) {
                matched.add(record);
            }
        }
        return matched;
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Test
    void reservedThenShippedFollowsTheLifecycle() {
        String orderId = newOrder();
        String reservedId = id();
        send(INVENTORY_TOPIC, orderId, reserved(reservedId, orderId));
        awaitState(orderId, "FULFILLMENT_REQUESTED");
        given().when().get("/api/v1/orders/" + orderId).then().statusCode(200)
                .body("reservationId", org.hamcrest.Matchers.is("res_" + orderId.substring(4)));
        assertEquals(1, count(FULFILLMENT, orderId));
        send(FULFILLMENT_TOPIC, orderId, shipped(id(), orderId));
        awaitState(orderId, "SHIPPED");
        assertEquals(List.of("ORDER_PLACED", "INVENTORY_RESERVED", "INVENTORY_RESERVED", "SHIPMENT_CONFIRMED"), reasons(orderId));
        List<Integer> sequence = given().when().get("/api/v1/orders/" + orderId + "/timeline").then().extract().path("sequence");
        List<Integer> sorted = new ArrayList<>(sequence);
        sorted.sort(null);
        assertEquals(sorted, sequence, "timeline order is stable");
        String event = given().when().get("/api/v1/orders/" + orderId + "/timeline").then().extract().path("[1].eventId");
        assertEquals(reservedId, event, "transition is attributable to its event");
    }

    @Test
    void redeliveryDoesNotDuplicateFulfillmentOrHistory() {
        String orderId = newOrder();
        String eventId = id();
        for (int i = 0; i < 5; i++) {
            send(INVENTORY_TOPIC, orderId, reserved(eventId, orderId));
        }
        awaitState(orderId, "FULFILLMENT_REQUESTED");
        // A new event ID for the same reservation is a separate delivery of a duplicate business event.
        String second = id();
        send(INVENTORY_TOPIC, orderId, reserved(second, orderId));
        awaitProcessed(second);
        assertEquals(1, count(FULFILLMENT, orderId));
        assertEquals(1, count(NOTES, orderId), "the second business event is noted; exact redelivery is not");
        assertEquals(3, count("SELECT count(*) FROM order_state_history WHERE order_id = ? AND kind = 'TRANSITION'", orderId));
    }

    @Test
    void outOfOrderShipmentMovesToExceptionAndLateEventsDoNotRegress() {
        String orderId = newOrder();
        send(FULFILLMENT_TOPIC, orderId, shipped(id(), orderId));
        awaitState(orderId, "EXCEPTION");
        String late = id();
        send(INVENTORY_TOPIC, orderId, reserved(late, orderId));
        awaitProcessed(late);
        assertEquals("EXCEPTION", state(orderId));
        assertEquals(0, count(FULFILLMENT, orderId));
        assertTrue(reasons(orderId).contains("IGNORED_EXCEPTION"));
    }

    @Test
    void eventsAfterTerminalStatesOnlyAddNotes() {
        String rejectedOrder = newOrder();
        send(INVENTORY_TOPIC, rejectedOrder, rejected(id(), rejectedOrder));
        awaitState(rejectedOrder, "REJECTED");
        String lateReserved = id();
        send(INVENTORY_TOPIC, rejectedOrder, reserved(lateReserved, rejectedOrder));
        awaitProcessed(lateReserved);
        assertEquals("REJECTED", state(rejectedOrder));
        assertEquals(0, count(FULFILLMENT, rejectedOrder));
        assertEquals(List.of("ORDER_PLACED", "INSUFFICIENT_STOCK", "IGNORED_REJECTED"), reasons(rejectedOrder));

        String shippedOrder = newOrder();
        send(INVENTORY_TOPIC, shippedOrder, reserved(id(), shippedOrder));
        awaitState(shippedOrder, "FULFILLMENT_REQUESTED");
        send(FULFILLMENT_TOPIC, shippedOrder, shipped(id(), shippedOrder));
        awaitState(shippedOrder, "SHIPPED");
        String lateRejected = id();
        send(INVENTORY_TOPIC, shippedOrder, rejected(lateRejected, shippedOrder));
        awaitProcessed(lateRejected);
        assertEquals("SHIPPED", state(shippedOrder));
    }

    @Test
    void reservationForDifferentContentNeedsReconciliationAndRequestsNothing() {
        String orderId = newOrder();
        send(INVENTORY_TOPIC, orderId, reserved(id(), orderId).replace("\"quantity\":2", "\"quantity\":1"));
        awaitState(orderId, "EXCEPTION");
        assertTrue(reasons(orderId).contains("RESERVATION_MISMATCH"));
        assertEquals(0, count(FULFILLMENT, orderId));
    }

    @Test
    void replayedReservationPastItsExpiryRequestsNothing() {
        String orderId = newOrder();
        send(INVENTORY_TOPIC, orderId, reserved(id(), orderId).replace("2099-01-01T00:00:00Z", "2020-01-01T00:00:00Z"));
        awaitState(orderId, "EXCEPTION");
        assertTrue(reasons(orderId).contains("RESERVATION_EXPIRED"));
        assertEquals(0, count(FULFILLMENT, orderId));
    }

    @Test
    void malformedAndUnknownEventsGoToTheDeadLetterTopicAndDoNotStopConsumption() {
        String badKey = "bad-" + id();
        send(INVENTORY_TOPIC, badKey, "{not json");
        String invalidKey = "invalid-" + id();
        send(INVENTORY_TOPIC, invalidKey, Harness.envelope(id(), "inventory.reserved.v1", "ord_1", Map.of()));
        String unknownOrder = "ord_unknown" + id().replace("-", "");
        String unknownEvent = id();
        send(INVENTORY_TOPIC, unknownOrder, reserved(unknownEvent, unknownOrder));
        Harness.await("three dead letters", () -> !dlq(badKey).isEmpty() && !dlq(invalidKey).isEmpty() && !dlq(unknownOrder).isEmpty());
        ConsumerRecord<String, String> bad = dlq(badKey).get(0);
        assertEquals("{not json", bad.value(), "dead letter keeps the original value");
        assertEquals(INVENTORY_TOPIC, header(bad, "dead-letter-topic"));
        assertTrue(header(bad, "dead-letter-reason").contains("not valid JSON"));
        assertEquals(0, count("SELECT count(*) FROM processed_event WHERE event_id = ?::uuid", unknownEvent),
                "a poison event's dedupe record rolls back, so replay can succeed later");
        String orderId = newOrder();
        send(INVENTORY_TOPIC, orderId, reserved(id(), orderId));
        awaitState(orderId, "FULFILLMENT_REQUESTED");
    }

    @Test
    void retryExhaustionDeadLettersAndReplayRecovers() throws SQLException {
        String orderId = newOrder();
        String eventId = id();
        AtomicLong attemptsBefore = new AtomicLong(deadLettered());
        exec("CREATE OR REPLACE FUNCTION fail_processed() RETURNS trigger AS $$ BEGIN"
                + " IF NEW.event_id = '" + eventId + "' THEN RAISE EXCEPTION 'injected'; END IF;"
                + " RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER fail_processed_trigger BEFORE INSERT ON processed_event FOR EACH ROW EXECUTE FUNCTION fail_processed()");
        String value = reserved(eventId, orderId);
        try {
            send(INVENTORY_TOPIC, orderId, value);
            Harness.await("dead letter", () -> !dlq(orderId).isEmpty());
            ConsumerRecord<String, String> sent = dlq(orderId).get(0);
            assertEquals("Retries exhausted", header(sent, "dead-letter-reason"));
            assertTrue(header(sent, "dead-letter-cause") == null, "no driver text in dead-letter headers");
        } finally {
            exec("DROP TRIGGER fail_processed_trigger ON processed_event");
        }
        assertEquals("SUBMITTED", state(orderId));
        assertTrue(deadLettered() > attemptsBefore.get(), "dead-letter metric rose");
        // The runbook's replay: re-produce the dead-lettered value to its source topic.
        ConsumerRecord<String, String> letter = dlq(orderId).get(0);
        send(header(letter, "dead-letter-topic"), letter.key(), letter.value());
        awaitState(orderId, "FULFILLMENT_REQUESTED");
        assertEquals(1, count(FULFILLMENT, orderId));
    }

    private static long deadLettered() {
        String metrics = given().when().get("/q/metrics").asString();
        return (long) metrics.lines().filter(l -> l.startsWith("box1_events_consumed_total") && l.contains("dead_lettered"))
                .mapToDouble(l -> Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1))).sum();
    }

    @Test
    void historyIsImmutable() throws SQLException {
        String orderId = newOrder();
        try (java.sql.Connection c = Harness.db(); Statement s = c.createStatement()) {
            for (String sql : List.of("UPDATE order_state_history SET reason_code = 'X' WHERE order_id = '" + orderId + "'",
                    "DELETE FROM order_state_history WHERE order_id = '" + orderId + "'", "TRUNCATE order_state_history CASCADE")) {
                SQLException error = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> s.execute(sql), sql);
                assertTrue(error.getMessage().contains("immutable"), error.getMessage());
            }
        }
    }

    @Test
    void customerDataStaysOutOfLogsAndLaterEvents() {
        List<String> captured = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.logging.Handler handler = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord record) {
                captured.add(new java.util.logging.SimpleFormatter().formatMessage(record));
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        java.util.logging.Logger root = java.util.logging.Logger.getLogger("");
        root.addHandler(handler);
        String secretKey = "key-" + id();
        String orderId;
        try {
            orderId = given().header("Idempotency-Key", secretKey).header("X-Consumer-Id", "consumer-private")
                    .contentType("application/json").body(Harness.body("cust_private_123")).post("/api/v1/orders")
                    .then().statusCode(201).extract().path("orderId");
            send(INVENTORY_TOPIC, orderId, reserved(id(), orderId));
            awaitState(orderId, "FULFILLMENT_REQUESTED");
            send(FULFILLMENT_TOPIC, orderId, shipped(id(), orderId));
            awaitState(orderId, "SHIPPED");
        } finally {
            root.removeHandler(handler);
        }
        assertTrue(captured.stream().anyMatch(m -> m.contains("Order created")), "the capture saw service logs");
        for (String message : captured) {
            assertTrue(!message.contains("cust_private_123") && !message.contains(secretKey) && !message.contains("consumer-private"), message);
        }
        String published = orderId;
        Harness.await("all published", () -> count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NULL", published) == 0);
        for (JsonNode event : Harness.orderEvents(orderId, Duration.ofSeconds(3))) {
            boolean carriesCustomer = event.toString().contains("cust_private_123");
            assertEquals("order.placed.v1".equals(event.get("eventType").asText()), carriesCustomer, event.get("eventType").asText());
        }
    }
}
