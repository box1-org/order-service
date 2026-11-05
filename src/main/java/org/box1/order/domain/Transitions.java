package org.box1.order.domain;

import java.util.List;
import java.util.Set;

/** The guarded transition table, published as ORDER-STATE.md in box1-ops-contracts. */
public final class Transitions {
    private static final Set<OrderState> CANCELLABLE = Set.of(OrderState.SUBMITTED);

    private Transitions() {}

    /** Whether a cancellation would be safe. No v1 endpoint exposes it. */
    public static boolean canCancel(OrderState state) {
        return CANCELLABLE.contains(state);
    }

    /**
     * Decide the effect of an event. An empty path means no state change, and the event becomes a timeline note.
     *
     * @param state current state
     * @param event validated event
     * @param policy completion policy
     * @param expectedFulfillmentRequestId the order's fulfillment request, or null
     * @param expectedReservationId the order's reservation, or null
     */
    public static Decision decide(OrderState state, LifecycleEvent event, CompletionPolicy policy,
            String expectedFulfillmentRequestId, String expectedReservationId) {
        String type = event.eventType();
        if (state == OrderState.SUBMITTED) {
            return switch (type) {
                case LifecycleEvent.RESERVED -> new Decision(
                        List.of(OrderState.RESERVED, OrderState.FULFILLMENT_REQUESTED), "INVENTORY_RESERVED", true);
                case LifecycleEvent.REJECTED -> Decision.to(OrderState.REJECTED, event.reasonCode());
                case LifecycleEvent.SHIPPED -> Decision.to(OrderState.EXCEPTION, "SHIPMENT_BEFORE_RESERVATION");
                default -> Decision.note("IGNORED_SUBMITTED");
            };
        }
        if (state == OrderState.FULFILLMENT_REQUESTED) {
            return switch (type) {
                case LifecycleEvent.SHIPPED -> matches(event.fulfillmentRequestId(), expectedFulfillmentRequestId)
                        ? shipped(policy) : Decision.note("FULFILLMENT_REQUEST_MISMATCH");
                case LifecycleEvent.FAILED -> failed(event, expectedFulfillmentRequestId);
                case LifecycleEvent.EXPIRED -> matches(event.reservationId(), expectedReservationId)
                        ? Decision.to(OrderState.EXCEPTION, "RESERVATION_EXPIRED")
                        : Decision.note("RESERVATION_MISMATCH");
                default -> Decision.note("IGNORED_FULFILLMENT_REQUESTED");
            };
        }
        return Decision.note("IGNORED_" + state.name());
    }

    private static boolean matches(String actual, String expected) {
        return expected != null && expected.equals(actual);
    }

    private static Decision shipped(CompletionPolicy policy) {
        return policy == CompletionPolicy.ON_SHIPMENT
                ? new Decision(List.of(OrderState.SHIPPED, OrderState.COMPLETED), "SHIPMENT_CONFIRMED", false)
                : Decision.to(OrderState.SHIPPED, "SHIPMENT_CONFIRMED");
    }

    private static Decision failed(LifecycleEvent event, String expectedFulfillmentRequestId) {
        if (!matches(event.fulfillmentRequestId(), expectedFulfillmentRequestId)) {
            return Decision.note("FULFILLMENT_REQUEST_MISMATCH");
        }
        return event.retryable() ? Decision.note("SHIPMENT_RETRYING") : Decision.to(OrderState.EXCEPTION, "SHIPMENT_FAILED");
    }

    /** Explicit policy for moving SHIPPED to COMPLETED. */
    public enum CompletionPolicy {
        NONE, ON_SHIPMENT;

        /** Parse the configured value. */
        public static CompletionPolicy parse(String value) {
            return switch (value) {
                case "none" -> NONE;
                case "on-shipment" -> ON_SHIPMENT;
                default -> throw new IllegalArgumentException("Unsupported order completion policy");
            };
        }
    }

    /** States to enter in order, the reason, and whether a fulfillment request must be enqueued. */
    public record Decision(List<OrderState> path, String reason, boolean requestFulfillment) {
        public Decision {
            path = List.copyOf(path);
        }

        static Decision to(OrderState state, String reason) {
            return new Decision(List.of(state), reason, false);
        }

        static Decision note(String reason) {
            return new Decision(List.of(), reason, false);
        }
    }
}
