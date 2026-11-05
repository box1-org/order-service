package org.box1.order;

import static io.restassured.RestAssured.given;
import static org.box1.order.Harness.body;
import static org.box1.order.Harness.count;
import static org.box1.order.Harness.exec;
import static org.box1.order.Harness.post;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.Response;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ContainersTag
class ApiScenariosTest {
    private static final String BY_ORDER = "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = 'order.placed.v1'";

    @Test
    void tenIdenticalPostsMakeOneOrderAndOneOutboxEvent() {
        String key = UUID.randomUUID().toString();
        Response first = post(key, body("cust_ten"));
        first.then().statusCode(201).header("Location", endsWith("/api/v1/orders/" + first.path("orderId")));
        String orderId = first.path("orderId");
        for (int i = 0; i < 9; i++) {
            post(key, body("cust_ten")).then().statusCode(200).header("Idempotency-Replayed", "true")
                    .body("orderId", equalTo(orderId));
        }
        assertEquals(1, count("SELECT count(*) FROM orders WHERE customer_id = 'cust_ten'"));
        assertEquals(1, count(BY_ORDER, orderId));
    }

    @Test
    void firstResponseAndLaterReadsCarryTheSameTimestamps() {
        Response created = post(UUID.randomUUID().toString(), body("cust_time"));
        String orderId = created.path("orderId");
        given().when().get("/api/v1/orders/" + orderId).then().statusCode(200)
                .body("createdAt", is((String) created.path("createdAt")));
    }

    @Test
    void sameKeyWithChangedBodyConflicts() {
        String key = UUID.randomUUID().toString();
        post(key, body("cust_conflict")).then().statusCode(201);
        post(key, body("cust_other")).then().statusCode(409).contentType(startsWith("application/problem+json"))
                .body("status", is(409)).body("title", is("Conflict"));
        assertEquals(0, count("SELECT count(*) FROM orders WHERE customer_id = 'cust_other'"));
    }

    @Test
    void formattingDoesNotChangeTheFingerprint() {
        String key = UUID.randomUUID().toString();
        post(key, body("cust_format")).then().statusCode(201);
        post(key, "{ \"siteId\":\"site_nyc\", \"quantity\":2, \"sku\":\"SKU-42\", \"customerId\":\"cust_format\" }")
                .then().statusCode(200);
    }

    @Test
    void scopesSeparateKeys() {
        String key = UUID.randomUUID().toString();
        String a = given().header("Idempotency-Key", key).header("X-Consumer-Id", "a").contentType("application/json")
                .body(body("cust_scope")).post("/api/v1/orders").then().statusCode(201).extract().path("orderId");
        String b = given().header("Idempotency-Key", key).header("X-Consumer-Id", "b").contentType("application/json")
                .body(body("cust_scope")).post("/api/v1/orders").then().statusCode(201).extract().path("orderId");
        assertTrue(!a.equals(b));
    }

    @Test
    void concurrentDuplicatesPersistOneAggregate() throws Exception {
        String key = UUID.randomUUID().toString();
        int threads = 24;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Response>> calls = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                calls.add(pool.submit(() -> {
                    start.await();
                    return post(key, body("cust_race"));
                }));
            }
            start.countDown();
            Set<String> ids = new HashSet<>();
            int created = 0;
            for (Future<Response> call : calls) {
                Response response = call.get();
                assertTrue(response.statusCode() == 201 || response.statusCode() == 200, "status " + response.statusCode());
                created += response.statusCode() == 201 ? 1 : 0;
                ids.add(response.path("orderId"));
            }
            assertEquals(1, created);
            assertEquals(1, ids.size());
            assertEquals(1, count("SELECT count(*) FROM orders WHERE customer_id = 'cust_race'"));
            assertEquals(1, count(BY_ORDER, ids.iterator().next()));
        }
    }

    @Test
    void duplicateBlockedAtCommitBoundarySeesTheWinner() throws Exception {
        // The first transaction pauses just before commit, while its duplicate waits on the unique index.
        exec("""
                CREATE OR REPLACE FUNCTION pause_boundary() RETURNS trigger AS $$
                BEGIN
                  IF NEW.customer_id = 'cust_boundary' THEN PERFORM pg_sleep(1.5); END IF;
                  RETURN NEW;
                END; $$ LANGUAGE plpgsql""");
        exec("CREATE TRIGGER pause_boundary_trigger AFTER INSERT ON orders FOR EACH ROW EXECUTE FUNCTION pause_boundary()");
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            String key = UUID.randomUUID().toString();
            Future<Response> first = pool.submit(() -> post(key, body("cust_boundary")));
            Thread.sleep(500);
            Future<Response> second = pool.submit(() -> post(key, body("cust_boundary")));
            assertEquals(201, first.get().statusCode());
            assertEquals(200, second.get().statusCode());
            assertEquals((String) first.get().path("orderId"), (String) second.get().path("orderId"));
        } finally {
            exec("DROP TRIGGER pause_boundary_trigger ON orders");
        }
    }

    @Test
    void failureDuringCreateRollsEverythingBack() {
        exec("""
                CREATE OR REPLACE FUNCTION fail_outbox() RETURNS trigger AS $$
                BEGIN
                  IF NEW.payload -> 'payload' ->> 'customerId' = 'cust_rollback' THEN RAISE EXCEPTION 'injected'; END IF;
                  RETURN NEW;
                END; $$ LANGUAGE plpgsql""");
        exec("CREATE TRIGGER fail_outbox_trigger BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION fail_outbox()");
        String key = UUID.randomUUID().toString();
        try {
            post(key, body("cust_rollback")).then().statusCode(503).contentType(startsWith("application/problem+json"))
                    .body("detail", is("The order store is unavailable"));
            assertEquals(0, count("SELECT count(*) FROM orders WHERE customer_id = 'cust_rollback'"));
            assertEquals(0, count("SELECT count(*) FROM idempotency_records WHERE idempotency_key = ?", key));
        } finally {
            exec("DROP TRIGGER fail_outbox_trigger ON outbox");
        }
        // The key is free again because nothing committed.
        post(key, body("cust_rollback")).then().statusCode(201);
    }

    @Test
    void validationAndMissingResourcesReturnProblems() {
        String valid = body("cust_valid");
        given().contentType("application/json").body(valid).post("/api/v1/orders").then().statusCode(400)
                .contentType(startsWith("application/problem+json"));
        String[] invalid = {
            valid.replace("cust_valid", "nope"), valid.replace("\"quantity\":2", "\"quantity\":0"),
            valid.replace("\"quantity\":2", "\"quantity\":\"2\""), valid.replace("\"quantity\":2", "\"quantity\":2.5"),
            valid.replace("site_nyc", "nyc"), valid.replace("SKU-42", ""), valid.replace("}", ",\"extra\":1}"),
            "{not json", "null", valid.replace("cust_valid", "cust_" + "x".repeat(60)),
            valid.replace("site_nyc", "site_" + "x".repeat(60)), valid.replace("SKU-42", "a\\u0000b"),
        };
        for (String body : invalid) {
            post(UUID.randomUUID().toString(), body).then().statusCode(400)
                    .contentType(startsWith("application/problem+json")).body("status", is(400));
        }
        post("k".repeat(256), valid).then().statusCode(400);
        post(UUID.randomUUID().toString(), valid.replace("cust_valid", "cust_" + "x".repeat(20000))).then().statusCode(413);
        given().when().get("/api/v1/orders/ord_missing").then().statusCode(404)
                .contentType(startsWith("application/problem+json")).body("title", is("Not Found"));
        given().when().get("/api/v1/orders/ord_missing/timeline").then().statusCode(404);
        given().when().get("/api/v1/orders/not-an-id").then().statusCode(404);
        given().queryParam("limit", 0).when().get("/api/v1/orders").then().statusCode(400);
        given().queryParam("limit", 101).when().get("/api/v1/orders").then().statusCode(400);
        given().queryParam("cursor", "%%%").when().get("/api/v1/orders").then().statusCode(400);
    }

    @Test
    void paginationIsStableWhileOrdersArrive() {
        for (int i = 0; i < 5; i++) {
            Harness.newOrder();
        }
        Response page1 = given().queryParam("limit", 2).when().get("/api/v1/orders");
        page1.then().statusCode(200).header("Link", containsString("rel=\"next\""));
        List<String> first = page1.path("orderId");
        String link = page1.header("Link");
        String next = link.substring(1, link.indexOf('>'));
        Harness.newOrder();
        List<String> second = given().when().get(next).then().statusCode(200).extract().path("orderId");
        assertEquals(2, second.size());
        assertTrue(first.stream().noneMatch(second::contains), "pages must not overlap after a new insert");
    }

    @Test
    void traceAndCorrelationPropagateFromTheRequest() {
        String trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        Response created = given().header("Idempotency-Key", UUID.randomUUID().toString())
                .header("traceparent", "00-" + trace + "-00f067aa0ba902b7-01").header("X-Correlation-Id", "corr-api")
                .contentType("application/json").body(body("cust_trace")).post("/api/v1/orders");
        created.then().statusCode(201).body("traceId", is(trace)).body("correlationId", is("corr-api"))
                .header("X-Correlation-Id", "corr-api");
        String orderId = created.path("orderId");
        given().when().get("/api/v1/orders/" + orderId + "/timeline").then().statusCode(200)
                .body("[0].toState", is("SUBMITTED")).body("[0].traceId", is(trace))
                .body("[0].causationId", startsWith("cmd_")).body("[0].occurredAt", notNullValue());
        assertEquals(1, count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND payload ->> 'traceparent' LIKE ?",
                orderId, "00-" + trace + "-%"));
        given().header("Idempotency-Key", UUID.randomUUID().toString()).header("X-Correlation-Id", "bad value!")
                .contentType("application/json").body(body("cust_trace2")).post("/api/v1/orders").then().statusCode(201)
                .body("correlationId", org.hamcrest.Matchers.not("bad value!"));
    }

    @Test
    void internalPathsDoNotExist() {
        for (String path : List.of("/internal/v1/orders", "/api/v1/orders/internal/replay", "/admin", "/q/dev")) {
            given().when().post(path).then().statusCode(org.hamcrest.Matchers.anyOf(is(404), is(405)));
        }
    }

    @Test
    void readinessIncludesTheDatabase() {
        given().when().get("/q/health/ready").then().statusCode(200).body(containsString("\"database\""));
    }

    @Test
    void metricsAreInspectable() {
        Harness.newOrder();
        String metrics = given().when().get("/q/metrics").asString();
        for (String name : List.of("box1_outbox_pending", "box1_outbox_oldest_pending_age_seconds", "box1_orders_stuck",
                "box1_orders_oldest_open_age_seconds", "box1_orders_lifecycle_total")) {
            assertTrue(metrics.contains(name), name);
        }
    }

    @Test
    void openApiExposesTheOrderOperations() {
        given().accept("application/json").when().get("/q/openapi").then().statusCode(200)
                .body("paths.'/api/v1/orders/{orderId}/timeline'.get.operationId", is("getOrderTimeline"));
    }
}
