package org.box1.order.domain;

import java.time.Instant;

/** The order aggregate as read from storage. */
public record Order(
        String orderId,
        String customerId,
        String sku,
        int quantity,
        String siteId,
        OrderState state,
        String statusReason,
        String reservationId,
        String fulfillmentRequestId,
        String correlationId,
        String traceId,
        Instant createdAt,
        Instant updatedAt) {}
