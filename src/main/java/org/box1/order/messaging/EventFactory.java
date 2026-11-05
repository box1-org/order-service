package org.box1.order.messaging;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.order.domain.Order;
import org.box1.order.domain.OrderState;
import org.box1.order.store.OutboxStore;

/** Builds contract envelopes and enqueues them in the caller's transaction. */
@ApplicationScoped
public class EventFactory {
    private final ObjectMapper mapper;
    private final OutboxStore outbox;

    public EventFactory(ObjectMapper mapper, OutboxStore outbox) {
        this.mapper = mapper;
        this.outbox = outbox;
    }

    /** Enqueue order.placed.v1. This is the only event that carries customerId. */
    public void orderPlaced(Order order, String causationId, String traceparent, Instant now) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", order.orderId());
        payload.put("customerId", order.customerId());
        payload.put("sku", order.sku());
        payload.put("quantity", order.quantity());
        payload.put("siteId", order.siteId());
        enqueue("order.placed.v1", order.orderId(), order.correlationId(), causationId, traceparent, payload, now);
    }

    /** Enqueue fulfillment.requested.v1. */
    public void fulfillmentRequested(Order order, String reservationId, String fulfillmentRequestId, String causationId,
            String traceparent, Instant now) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", order.orderId());
        payload.put("reservationId", reservationId);
        payload.put("fulfillmentRequestId", fulfillmentRequestId);
        payload.put("siteId", order.siteId());
        payload.put("sku", order.sku());
        payload.put("quantity", order.quantity());
        enqueue("fulfillment.requested.v1", order.orderId(), order.correlationId(), causationId, traceparent, payload, now);
    }

    /** Enqueue order.state-changed.v1. */
    public void stateChanged(Order order, OrderState from, OrderState to, String reason, String causationId,
            String traceparent, Instant now) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", order.orderId());
        payload.put("fromState", from.name());
        payload.put("toState", to.name());
        payload.put("reasonCode", reason);
        payload.put("changedAt", now.toString());
        enqueue("order.state-changed.v1", order.orderId(), order.correlationId(), causationId, traceparent, payload, now);
    }

    private void enqueue(String type, String orderId, String correlationId, String causationId, String traceparent,
            ObjectNode payload, Instant now) {
        UUID eventId = UUID.randomUUID();
        outbox.enqueue(eventId, orderId, type, envelope(eventId, type, orderId, correlationId, causationId, traceparent,
                payload, now).toString());
    }

    /** Build an envelope matching events/v1/envelope.schema.json. */
    ObjectNode envelope(UUID eventId, String type, String orderId, String correlationId, String causationId,
            String traceparent, ObjectNode payload, Instant now) {
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("eventId", eventId.toString());
        envelope.put("eventType", type);
        envelope.put("schemaVersion", 1);
        envelope.put("aggregateId", orderId);
        envelope.put("occurredAt", now.toString());
        envelope.put("correlationId", correlationId);
        envelope.put("causationId", causationId);
        if (traceparent != null) {
            envelope.put("traceparent", traceparent);
        }
        envelope.set("payload", payload);
        return envelope;
    }
}
