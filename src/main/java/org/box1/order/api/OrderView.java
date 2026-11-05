package org.box1.order.api;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.box1.order.domain.Order;
import org.box1.order.domain.OrderState;

/** Order as returned by the API. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderView(
        String orderId,
        OrderState state,
        String customerId,
        String sku,
        int quantity,
        String siteId,
        String reservationId,
        String statusReason,
        Instant createdAt,
        Instant updatedAt,
        String correlationId,
        String traceId) {

    static OrderView of(Order order) {
        return new OrderView(order.orderId(), order.state(), order.customerId(), order.sku(), order.quantity(),
                order.siteId(), order.reservationId(), order.statusReason(), order.createdAt(), order.updatedAt(),
                order.correlationId(), order.traceId());
    }
}
