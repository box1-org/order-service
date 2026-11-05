package org.box1.order.domain;

/** Order lifecycle states from the shared contract. */
public enum OrderState {
    SUBMITTED, RESERVED, FULFILLMENT_REQUESTED, SHIPPED, COMPLETED, REJECTED, EXCEPTION, CANCELLED;

    /** Whether the order still expects lifecycle progress, so it can become stuck. */
    public boolean isOpen() {
        return this == SUBMITTED || this == RESERVED || this == FULFILLMENT_REQUESTED;
    }
}
