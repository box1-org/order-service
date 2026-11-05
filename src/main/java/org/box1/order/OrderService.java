package org.box1.order;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.order.domain.Fingerprint;
import org.box1.order.domain.LifecycleEvent;
import org.box1.order.domain.Order;
import org.box1.order.domain.OrderState;
import org.box1.order.domain.Transitions;
import org.box1.order.messaging.EventFactory;
import org.box1.order.messaging.PoisonEventException;
import org.box1.order.store.OrderStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Order commands and event effects. Each public method is one business transaction. */
@ApplicationScoped
public class OrderService {
    private static final String TRANSITION = "TRANSITION";
    private static final String NOTE = "NOTE";

    private final OrderStore store;
    private final EventFactory events;
    private final Transitions.CompletionPolicy policy;

    public OrderService(OrderStore store, EventFactory events,
            @ConfigProperty(name = "order.completion.policy") String policy) {
        this.store = store;
        this.events = events;
        this.policy = Transitions.CompletionPolicy.parse(policy);
    }

    /** Create an order, or return the original for a replayed key. See decisions D-007 and D-008. */
    @Transactional
    public CreateResult create(CreateCommand command) {
        String hash = Fingerprint.of(command.customerId(), command.sku(), command.quantity(), command.siteId());
        String orderId = "ord_" + UUID.randomUUID().toString().replace("-", "");
        Instant now = Instant.now();
        if (!store.claimKey(command.scope(), command.idempotencyKey(), hash, orderId, now)) {
            OrderStore.KeyRecord existing = store.findKey(command.scope(), command.idempotencyKey())
                    .orElseThrow(() -> new IllegalStateException("Idempotency record vanished"));
            if (!existing.requestHash().equals(hash)) {
                throw new IdempotencyConflictException();
            }
            Order original = store.find(existing.orderId())
                    .orElseThrow(() -> new IllegalStateException("Idempotency record has no order"));
            return new CreateResult(original, false);
        }
        String commandId = "cmd_" + UUID.randomUUID();
        SpanContext span = Span.current().getSpanContext();
        String traceId = span.isValid() ? span.getTraceId() : null;
        Order order = new Order(orderId, command.customerId(), command.sku(), command.quantity(), command.siteId(),
                OrderState.SUBMITTED, "ORDER_PLACED", null, null, command.correlationId(), traceId, now, now);
        store.insertOrder(order);
        store.appendHistory(orderId, TRANSITION, null, OrderState.SUBMITTED, "ORDER_PLACED", now, null, null, commandId,
                command.correlationId(), traceId);
        events.orderPlaced(order, commandId, traceparent(), now);
        return new CreateResult(order, true);
    }

    /** Apply one event exactly once per eventId, in one transaction with its dedupe record and outbox rows. */
    @Transactional
    public Applied apply(LifecycleEvent event) {
        Instant now = Instant.now();
        if (!store.markProcessed(event.eventId(), event.eventType(), now)) {
            return new Applied("duplicate", List.of());
        }
        Order order = store.lock(event.orderId()).orElseThrow(() -> new PoisonEventException("Unknown order"));
        Transitions.Decision decision = Transitions.decide(order.state(), event, policy, order.fulfillmentRequestId(),
                order.reservationId());
        String causation = event.eventId().toString();
        SpanContext span = Span.current().getSpanContext();
        String traceId = span.isValid() ? span.getTraceId() : order.traceId();
        if (decision.path().isEmpty()) {
            store.appendHistory(order.orderId(), NOTE, null, null, decision.reason(), now, event.eventId(),
                    event.eventType(), causation, order.correlationId(), traceId);
            return new Applied("noted", List.of());
        }
        String reservationId = event.eventType().equals(LifecycleEvent.RESERVED) ? event.reservationId() : null;
        String fulfillmentRequestId = decision.requestFulfillment()
                ? "ful_" + order.orderId().substring("ord_".length()) : null;
        String traceparent = traceparent();
        OrderState from = order.state();
        List<OrderState> entered = new ArrayList<>();
        for (OrderState to : decision.path()) {
            store.updateState(order.orderId(), to, decision.reason(), reservationId, fulfillmentRequestId, now);
            store.appendHistory(order.orderId(), TRANSITION, from, to, decision.reason(), now, event.eventId(),
                    event.eventType(), causation, order.correlationId(), traceId);
            if (to == OrderState.FULFILLMENT_REQUESTED && decision.requestFulfillment()) {
                events.fulfillmentRequested(order, event.reservationId(), fulfillmentRequestId, causation, traceparent, now);
            }
            events.stateChanged(order, from, to, decision.reason(), causation, traceparent, now);
            from = to;
            entered.add(to);
        }
        return new Applied("applied", entered);
    }

    /** Read an order. */
    public Optional<Order> find(String orderId) {
        return store.find(orderId);
    }

    private static String traceparent() {
        SpanContext span = Span.current().getSpanContext();
        return span.isValid()
                ? "00-" + span.getTraceId() + "-" + span.getSpanId() + "-" + span.getTraceFlags().asHex()
                : null;
    }

    /** A validated create request. */
    public record CreateCommand(String scope, String idempotencyKey, String customerId, String sku, int quantity,
            String siteId, String correlationId) {}

    /** The order and whether this request created it. */
    public record CreateResult(Order order, boolean created) {}

    /** The outcome of applying an event and any states entered. */
    public record Applied(String outcome, List<OrderState> entered) {
        public Applied {
            entered = List.copyOf(entered);
        }
    }

    /** The same idempotency key was used with a different payload. */
    public static class IdempotencyConflictException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public IdempotencyConflictException() {
            super("Idempotency key was used with a different payload");
        }
    }
}
