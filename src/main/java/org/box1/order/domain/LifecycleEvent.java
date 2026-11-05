package org.box1.order.domain;

import java.time.Instant;
import java.util.UUID;

/** A validated inbound lifecycle event. Fields the event type does not carry are null. */
public record LifecycleEvent(
        UUID eventId,
        String eventType,
        String orderId,
        Instant occurredAt,
        String correlationId,
        String traceparent,
        String reservationId,
        String fulfillmentRequestId,
        boolean retryable,
        String reasonCode,
        String sku,
        Integer quantity,
        String siteId,
        Instant expiresAt) {

    public static final String RESERVED = "inventory.reserved.v1";
    public static final String REJECTED = "inventory.rejected.v1";
    public static final String EXPIRED = "inventory.reservation-expired.v1";
    public static final String SHIPPED = "shipment.confirmed.v1";
    public static final String FAILED = "shipment.failed.v1";
}
