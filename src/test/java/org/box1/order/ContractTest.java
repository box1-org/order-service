package org.box1.order;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.box1.order.domain.LifecycleEvent;
import org.box1.order.messaging.EventParser;
import org.box1.order.messaging.PoisonEventException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

/**
 * Validates against box1-ops-contracts at the commit pinned in pom.xml. The contracts are fetched, never copied, so
 * this repository holds no local schema. This test runs in CI, and a fetch failure fails it rather than skipping.
 */
@QuarkusTest
class ContractTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonSchemaFactory FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

    @Inject
    EventParser parser;

    private static String fetch(String path) throws IOException, InterruptedException {
        String ref = System.getProperty("contracts.ref");
        URI uri = URI.create("https://raw.githubusercontent.com/box1-org/box1-ops-contracts/" + ref + "/" + path);
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder(uri).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), path);
            return response.body();
        }
    }

    private static JsonSchema schema(String type) throws IOException, InterruptedException {
        SchemaValidatorsConfig config = SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build();
        return FACTORY.getSchema(fetch("events/v1/" + type + ".schema.json"), config);
    }

    @Test
    void parserAcceptsEveryConsumedFixture() throws Exception {
        Set<String> consumed = Set.of(LifecycleEvent.RESERVED, LifecycleEvent.REJECTED, LifecycleEvent.EXPIRED,
                LifecycleEvent.SHIPPED, LifecycleEvent.FAILED);
        int seen = 0;
        for (JsonNode fixture : JSON.readTree(fetch("fixtures/events.json"))) {
            if (consumed.contains(fixture.get("eventType").asText())) {
                assertEquals(fixture.get("eventType").asText(), parser.parse(fixture.toString()).eventType());
                seen++;
            }
        }
        assertEquals(consumed.size(), seen);
    }

    @Test
    void producedFixturesAndEnvelopeValidate() throws Exception {
        JsonSchema envelope = schema("envelope");
        for (JsonNode fixture : JSON.readTree(fetch("fixtures/events.json"))) {
            assertTrue(envelope.validate(fixture).isEmpty(), fixture.get("eventType").asText());
        }
    }

    @Test
    void malformedEnvelopesArePoison() {
        String[] bad = {
            "not json", "[]", "{}", null,
            Harness.envelope("not-a-uuid", LifecycleEvent.RESERVED, "ord_1", java.util.Map.of()),
            Harness.envelope(Harness.id(), "unknown.event.v1", "ord_1", java.util.Map.of()),
            Harness.envelope(Harness.id(), LifecycleEvent.RESERVED, "ord_1", java.util.Map.of()),
            Harness.envelope(Harness.id(), LifecycleEvent.FAILED, "ord_1", java.util.Map.of("fulfillmentRequestId", "ful_1",
                    "reasonCode", "X")),
            Harness.reserved(Harness.id(), "ord_1").replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
            Harness.reserved(Harness.id(), "ord_1").replace("\"aggregateId\":\"ord_1\"", "\"aggregateId\":\"ord_2\""),
            Harness.reserved(Harness.id(), "ord_1").replace("00-4bf9", "99-4bf9"),
            Harness.reserved(Harness.id(), "ord_1").replace("\"quantity\":2", "\"quantity\":0"),
            Harness.reserved(Harness.id(), "ord_1").replace("\"sku\":\"SKU-42\",", ""),
            Harness.rejected(Harness.id(), "ord_1").replace("INSUFFICIENT_STOCK", "free text with data"),
            Harness.shipped(Harness.id(), "ord_1").replace("shp_1", "other"),
        };
        for (String value : bad) {
            Assertions.assertThrows(PoisonEventException.class, () -> parser.parse(value), value);
        }
    }

    @Test
    void generatedOpenApiCoversTheContract() throws Exception {
        JsonNode generated = JSON.readTree(given().accept("application/json").when().get("/q/openapi").asString());
        JsonNode contract = new com.fasterxml.jackson.dataformat.yaml.YAMLMapper()
                .readTree(fetch("openapi/order-service.yaml"));
        int operations = 0;
        for (var path : (Iterable<java.util.Map.Entry<String, JsonNode>>) contract.get("paths")::fields) {
            for (var op : (Iterable<java.util.Map.Entry<String, JsonNode>>) path.getValue()::fields) {
                assertEquals(op.getValue().get("operationId").asText(),
                        generated.path("paths").path(path.getKey()).path(op.getKey()).path("operationId").asText(),
                        path.getKey() + " " + op.getKey());
                operations++;
            }
        }
        assertEquals(4, operations);
    }
}
