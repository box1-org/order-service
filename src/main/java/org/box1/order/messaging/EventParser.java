package org.box1.order.messaging;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.order.domain.LifecycleEvent;

/**
 * Validates inbound envelopes and payloads against the rules in box1-ops-contracts.
 * Failure messages name fields and never echo values, because they reach dead-letter headers.
 */
@ApplicationScoped
public class EventParser {
    private static final Pattern ORDER = Pattern.compile("^ord_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern RESERVATION = Pattern.compile("^res_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern FULFILLMENT = Pattern.compile("^ful_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern SHIPMENT = Pattern.compile("^shp_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern SITE = Pattern.compile("^site_[A-Za-z0-9_-]{1,120}$");
    private static final Pattern SKU = Pattern.compile("^[^\\p{Cntrl}]{1,80}$");
    private static final Pattern REASON = Pattern.compile("^[A-Z][A-Z0-9_]{0,63}$");
    private static final Pattern TRACEPARENT = Pattern.compile("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");
    private static final int MAX_ID = 128;

    private final ObjectMapper mapper;

    public EventParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** Parse a record value, or throw {@link PoisonEventException}. */
    public LifecycleEvent parse(String value) {
        JsonNode root = read(value);
        if (root.path("schemaVersion").asInt(-1) != 1) {
            throw new PoisonEventException("Unsupported schemaVersion");
        }
        UUID eventId = uuid(root);
        String type = text(root, "eventType");
        String aggregateId = bounded(root, "aggregateId");
        Instant occurredAt = instant(root, "occurredAt");
        String correlationId = bounded(root, "correlationId");
        bounded(root, "causationId");
        String traceparent = null;
        if (root.hasNonNull("traceparent")) {
            traceparent = matching(root, "traceparent", TRACEPARENT);
        }
        JsonNode payload = root.path("payload");
        if (!payload.isObject()) {
            throw new PoisonEventException("Missing payload");
        }
        String orderId = matching(payload, "orderId", ORDER);
        if (!orderId.equals(aggregateId)) {
            throw new PoisonEventException("aggregateId does not match orderId");
        }
        String reservationId = null;
        String fulfillmentRequestId = null;
        boolean retryable = false;
        String reasonCode = null;
        String sku = null;
        Integer quantity = null;
        String siteId = null;
        Instant expiresAt = null;
        switch (type) {
            case LifecycleEvent.RESERVED -> {
                reservationId = matching(payload, "reservationId", RESERVATION);
                expiresAt = instant(payload, "expiresAt");
                sku = matching(payload, "sku", SKU);
                quantity = quantity(payload);
                siteId = matching(payload, "siteId", SITE);
            }
            case LifecycleEvent.REJECTED -> {
                reasonCode = matching(payload, "reasonCode", REASON);
                sku = matching(payload, "sku", SKU);
                quantity = quantity(payload);
                siteId = matching(payload, "siteId", SITE);
            }
            case LifecycleEvent.EXPIRED -> {
                reservationId = matching(payload, "reservationId", RESERVATION);
                instant(payload, "expiredAt");
            }
            case LifecycleEvent.SHIPPED -> {
                fulfillmentRequestId = matching(payload, "fulfillmentRequestId", FULFILLMENT);
                matching(payload, "shipmentId", SHIPMENT);
                instant(payload, "shippedAt");
            }
            case LifecycleEvent.FAILED -> {
                fulfillmentRequestId = matching(payload, "fulfillmentRequestId", FULFILLMENT);
                reasonCode = matching(payload, "reasonCode", REASON);
                if (!payload.path("retryable").isBoolean()) {
                    throw new PoisonEventException("Missing retryable");
                }
                retryable = payload.get("retryable").asBoolean();
            }
            default -> throw new PoisonEventException("Unsupported eventType");
        }
        return new LifecycleEvent(eventId, type, orderId, occurredAt, correlationId, traceparent, reservationId,
                fulfillmentRequestId, retryable, reasonCode, sku, quantity, siteId, expiresAt);
    }

    private static Integer quantity(JsonNode payload) {
        JsonNode value = payload.get("quantity");
        if (value == null || !value.canConvertToInt() || !value.isIntegralNumber() || value.asInt() < 1) {
            throw new PoisonEventException("Invalid quantity");
        }
        return value.asInt();
    }

    private JsonNode read(String value) {
        if (value == null) {
            throw new PoisonEventException("Empty record value");
        }
        try {
            JsonNode root = mapper.readTree(value);
            if (root == null || !root.isObject()) {
                throw new PoisonEventException("Record value is not a JSON object");
            }
            return root;
        } catch (JsonProcessingException error) {
            throw new PoisonEventException("Record value is not valid JSON");
        }
    }

    private static UUID uuid(JsonNode root) {
        try {
            return UUID.fromString(text(root, "eventId"));
        } catch (IllegalArgumentException error) {
            throw new PoisonEventException("Invalid eventId");
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.asText().isEmpty()) {
            throw new PoisonEventException("Missing " + field);
        }
        return value.asText();
    }

    private static String bounded(JsonNode node, String field) {
        String value = text(node, field);
        if (value.length() > MAX_ID) {
            throw new PoisonEventException("Oversized " + field);
        }
        return value;
    }

    private static String matching(JsonNode node, String field, Pattern pattern) {
        String value = text(node, field);
        if (!pattern.matcher(value).matches()) {
            throw new PoisonEventException("Invalid " + field);
        }
        return value;
    }

    private static Instant instant(JsonNode node, String field) {
        try {
            return Instant.parse(text(node, field));
        } catch (DateTimeParseException error) {
            throw new PoisonEventException("Invalid " + field);
        }
    }
}
