package org.box1.order.domain;

import java.time.Instant;
import java.util.UUID;

/** One immutable entry in an order's history. */
public record TimelineEntry(
        long sequence,
        String kind,
        OrderState fromState,
        OrderState toState,
        String reasonCode,
        Instant occurredAt,
        UUID eventId,
        String eventType,
        String causationId,
        String correlationId,
        String traceId) {}
