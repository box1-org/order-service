package org.box1.order;

import static org.box1.order.Harness.count;
import static org.box1.order.Harness.exec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import org.box1.order.store.OutboxStore;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ContainersTag
class OutboxScenariosTest {
    private static final String PUBLISHED = "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL";
    private static final String PENDING = "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NULL";

    @Inject
    OutboxStore outbox;

    private static void pauseBroker() {
        Harness.pauseBroker();
    }

    private static void resumeBroker() {
        Harness.resumeBroker();
    }

    @Test
    void publishesCommittedOrderKeyedByOrderId() {
        String orderId = Harness.newOrder();
        Harness.await("publish", () -> count(PUBLISHED, orderId) == 1);
        List<JsonNode> events = Harness.orderEvents(orderId, Duration.ofSeconds(3));
        assertEquals(1, events.size());
        assertEquals("order.placed.v1", events.get(0).get("eventType").asText());
        assertEquals(orderId, events.get(0).get("aggregateId").asText());
    }

    @Test
    void brokerOutageKeepsOrdersAndRecoversOnce() {
        pauseBroker();
        String orderId;
        try {
            orderId = Harness.newOrder();
            String waiting = orderId;
            Harness.await("a failed attempt", () -> count(
                    "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND attempts > 0 AND last_error IS NOT NULL", waiting) == 1);
            assertEquals(1, count(PENDING, orderId), "unpublished while the broker is down");
            RestAssured.given().when().get("/q/health/ready").then().statusCode(200);
            RestAssured.given().when().get("/q/health/live").then().statusCode(200);
            Harness.await("pending gauge", () -> {
                String metrics = RestAssured.given().when().get("/q/metrics").asString();
                return metrics.lines().anyMatch(l -> l.startsWith("box1_outbox_pending") && !l.endsWith(" 0.0"));
            });
        } finally {
            resumeBroker();
        }
        String recovered = orderId;
        Harness.await("recovery publish", () -> count(PUBLISHED, recovered) == 1);
        List<JsonNode> events = Harness.orderEvents(orderId, Duration.ofSeconds(3));
        Set<String> ids = new HashSet<>();
        events.forEach(e -> ids.add(e.get("eventId").asText()));
        assertEquals(1, ids.size(), "one logical event");
    }

    @Test
    void crashAfterBrokerAckResendsTheSameEventId() {
        String orderId = Harness.newOrder();
        Harness.await("publish", () -> count(PUBLISHED, orderId) == 1);
        // Simulates a crash between broker confirmation and the database mark.
        exec("UPDATE outbox SET published_at = NULL WHERE aggregate_id = '" + orderId + "'");
        Harness.await("re-publish", () -> count(PUBLISHED, orderId) == 1);
        List<JsonNode> events = Harness.orderEvents(orderId, Duration.ofSeconds(3));
        assertEquals(2, events.size(), "at-least-once delivery produced a duplicate");
        assertEquals(events.get(0).get("eventId").asText(), events.get(1).get("eventId").asText());
    }

    @Test
    void expiredLeaseIsReclaimedAndLiveLeaseIsNot() {
        String orderId = Harness.newOrder();
        Harness.await("publish", () -> count(PUBLISHED, orderId) == 1);
        exec("UPDATE outbox SET published_at = NULL, claimed_by = 'crashed', claim_expires_at = now() + interval '2 seconds'"
                + " WHERE aggregate_id = '" + orderId + "'");
        // A live lease from a dead publisher blocks work until it expires, then another owner takes over.
        assertEquals(1, count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND claimed_by = 'crashed'", orderId));
        Harness.await("reclaim after lease expiry", () -> count(PUBLISHED, orderId) == 1);
    }

    @Test
    void concurrentClaimersNeverShareARow() throws Exception {
        // Stop background work from taking these rows by giving them a future attempt until the claimers run.
        List<String> orders = new ArrayList<>();
        exec("CREATE OR REPLACE FUNCTION hold_outbox() RETURNS trigger AS $$ BEGIN NEW.next_attempt_at := now() + interval '30 seconds';"
                + " RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER hold_outbox_trigger BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION hold_outbox()");
        try {
            for (int i = 0; i < 30; i++) {
                orders.add(Harness.newOrder());
            }
        } finally {
            exec("DROP TRIGGER hold_outbox_trigger ON outbox");
        }
        String list = "'" + String.join("','", orders) + "'";
        exec("UPDATE outbox SET next_attempt_at = now() WHERE aggregate_id IN (" + list + ")");
        List<Future<List<OutboxStore.Claimed>>> claims = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            for (int i = 0; i < 4; i++) {
                String owner = "claimer-" + i;
                claims.add(pool.submit(() -> outbox.claim(owner, 10, 60)));
            }
            Set<UUID> seen = new HashSet<>();
            for (Future<List<OutboxStore.Claimed>> claim : claims) {
                List<OutboxStore.Claimed> rows = claim.get();
                assertTrue(rows.size() <= 10, "batch is bounded");
                for (OutboxStore.Claimed row : rows) {
                    assertTrue(seen.add(row.eventId()), "row claimed twice");
                }
            }
        }
        exec("UPDATE outbox SET claimed_by = NULL, claim_expires_at = NULL WHERE aggregate_id IN (" + list + ")");
    }

    @Test
    void perOrderOrderingHoldsAcrossAFailedHead() {
        String orderId = Harness.newOrder();
        Harness.await("placed published", () -> count(PUBLISHED, orderId) == 1);
        exec("CREATE OR REPLACE FUNCTION delay_first() RETURNS trigger AS $$ BEGIN NEW.next_attempt_at := now() + interval '2 seconds';"
                + " RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER delay_first_trigger BEFORE INSERT ON outbox FOR EACH ROW WHEN (NEW.event_type = 'order.state-changed.v1')"
                + " EXECUTE FUNCTION delay_first()");
        try {
            Harness.send(Harness.INVENTORY_TOPIC, orderId, Harness.reserved(Harness.id(), orderId));
            Harness.await("three events published", () -> count(PUBLISHED, orderId) == 4);
        } finally {
            exec("DROP TRIGGER delay_first_trigger ON outbox");
        }
        List<JsonNode> events = Harness.orderEvents(orderId, Duration.ofSeconds(3));
        List<String> types = events.stream().map(e -> e.get("eventType").asText()).toList();
        assertEquals(List.of("order.placed.v1", "order.state-changed.v1", "fulfillment.requested.v1", "order.state-changed.v1"), types);
    }
}
