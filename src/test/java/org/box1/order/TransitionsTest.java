package org.box1.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.box1.order.domain.Fingerprint;
import org.box1.order.domain.LifecycleEvent;
import org.box1.order.domain.OrderState;
import org.box1.order.domain.Transitions;
import org.box1.order.domain.Transitions.CompletionPolicy;
import org.box1.order.domain.Transitions.Decision;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TransitionsTest {
    private static final String FUL = "ful_1";
    private static final String RES = "res_1";

    private static LifecycleEvent event(String type, boolean retryable) {
        return new LifecycleEvent(UUID.randomUUID(), type, "ord_1", Instant.now(), "c", null, RES, FUL, retryable, "WHY");
    }

    private static Decision decide(OrderState state, String type) {
        return Transitions.decide(state, event(type, false), CompletionPolicy.NONE, FUL, RES);
    }

    @Test
    void reservedRequestsFulfillmentOnce() {
        Decision decision = decide(OrderState.SUBMITTED, LifecycleEvent.RESERVED);
        assertEquals(List.of(OrderState.RESERVED, OrderState.FULFILLMENT_REQUESTED), decision.path());
        assertTrue(decision.requestFulfillment());
    }

    @Test
    void rejectedCarriesItsReason() {
        Decision decision = decide(OrderState.SUBMITTED, LifecycleEvent.REJECTED);
        assertEquals(List.of(OrderState.REJECTED), decision.path());
        assertEquals("WHY", decision.reason());
    }

    @Test
    void shipmentBeforeReservationNeedsReconciliation() {
        assertEquals(List.of(OrderState.EXCEPTION), decide(OrderState.SUBMITTED, LifecycleEvent.SHIPPED).path());
    }

    @Test
    void completionNeedsAnExplicitPolicy() {
        assertEquals(List.of(OrderState.SHIPPED), decide(OrderState.FULFILLMENT_REQUESTED, LifecycleEvent.SHIPPED).path());
        assertEquals(List.of(OrderState.SHIPPED, OrderState.COMPLETED), Transitions.decide(OrderState.FULFILLMENT_REQUESTED,
                event(LifecycleEvent.SHIPPED, false), CompletionPolicy.ON_SHIPMENT, FUL, RES).path());
    }

    @Test
    void retryableFailureOnlyAddsANote() {
        Decision retry = Transitions.decide(OrderState.FULFILLMENT_REQUESTED, event(LifecycleEvent.FAILED, true),
                CompletionPolicy.NONE, FUL, RES);
        assertTrue(retry.path().isEmpty());
        assertEquals(List.of(OrderState.EXCEPTION), decide(OrderState.FULFILLMENT_REQUESTED, LifecycleEvent.FAILED).path());
    }

    @Test
    void expiredReservationAfterFulfillmentNeedsReconciliation() {
        assertEquals(List.of(OrderState.EXCEPTION), decide(OrderState.FULFILLMENT_REQUESTED, LifecycleEvent.EXPIRED).path());
        assertTrue(decide(OrderState.SUBMITTED, LifecycleEvent.EXPIRED).path().isEmpty());
    }

    @Test
    void mismatchedIdentifiersCannotMoveAnOrder() {
        for (String type : List.of(LifecycleEvent.SHIPPED, LifecycleEvent.FAILED, LifecycleEvent.EXPIRED)) {
            Decision decision = Transitions.decide(OrderState.FULFILLMENT_REQUESTED, event(type, false),
                    CompletionPolicy.NONE, "ful_other", "res_other");
            assertTrue(decision.path().isEmpty(), type);
        }
    }

    @ParameterizedTest
    @EnumSource(value = OrderState.class, names = {"RESERVED", "SHIPPED", "COMPLETED", "REJECTED", "EXCEPTION", "CANCELLED"})
    void statesOutsideTheTableNeverMove(OrderState state) {
        for (String type : List.of(LifecycleEvent.RESERVED, LifecycleEvent.REJECTED, LifecycleEvent.EXPIRED,
                LifecycleEvent.SHIPPED, LifecycleEvent.FAILED)) {
            assertTrue(decide(state, type).path().isEmpty(), state + " " + type);
        }
    }

    @Test
    void cancelOnlyWhenSafe() {
        for (OrderState state : OrderState.values()) {
            assertEquals(state == OrderState.SUBMITTED, Transitions.canCancel(state), state.name());
        }
    }

    @Test
    void fingerprintIsByValueAndUnambiguous() {
        assertEquals(Fingerprint.of("cust_1", "A", 1, "site_1"), Fingerprint.of("cust_1", "A", 1, "site_1"));
        assertNotEquals(Fingerprint.of("cust_1", "A", 1, "site_1"), Fingerprint.of("cust_1", "A", 2, "site_1"));
        assertNotEquals(Fingerprint.of("cust_1", "AB", 1, "site_1"), Fingerprint.of("cust_1A", "B", 1, "site_1"));
        assertFalse(Fingerprint.of("cust_1", "A", 1, "site_1").contains("cust"));
    }
}
