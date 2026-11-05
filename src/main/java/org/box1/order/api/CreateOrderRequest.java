package org.box1.order.api;

/** Request body. quarkus.jackson.fail-on-unknown-properties rejects extra fields, as the contract requires. */
public record CreateOrderRequest(String customerId, String sku, Integer quantity, String siteId) {}
