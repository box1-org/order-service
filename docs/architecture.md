# Architecture

The order service owns order commands, authoritative order state, idempotency, the order timeline and the order event outbox. An order is a business aggregate, not a distributed transaction. The service reacts to inventory and fulfillment events and never reads another service's tables.

Shared contracts live in [`box1-ops-contracts`](https://github.com/box1-org/box1-ops-contracts). The contract tests fetch them at the commit pinned in `pom.xml`, so this repository keeps no local copy of a shared schema.

## Create

`POST /api/v1/orders` runs in one READ COMMITTED transaction:

1. Insert the idempotency record with `ON CONFLICT DO NOTHING`.
2. Insert the order, its first history row and the `order.placed.v1` outbox row.

A concurrent duplicate waits on the unique index until the first transaction ends. If that commits, the duplicate reads the winner's record. If it rolled back, the duplicate proceeds. Responses:

| Request | Response |
|---|---|
| New key | `201` with `Location` |
| Same key, same payload | `200` with the original order and `Idempotency-Replayed: true` |
| Same key, different payload | `409` Problem |

The fingerprint covers field values, not JSON formatting. Records are kept for the life of the order.

## State

The transition table is [`ORDER-STATE.md`](https://github.com/box1-org/box1-ops-contracts/blob/main/ORDER-STATE.md), implemented in `Transitions`.

- `order_state_history` rejects UPDATE, DELETE and TRUNCATE with a trigger. Each row names its event, cause, correlation ID and trace ID.
- Events that cannot apply add a timeline note and change nothing. Exact redelivery adds nothing.
- `COMPLETED` needs `ORDER_COMPLETION_POLICY=on-shipment`. The default is `none`, so orders rest at `SHIPPED`.
- `CANCELLED` is legal only from `SUBMITTED`, but no v1 endpoint performs it.

## Outbox

Business writes and broker sends are separate transactions:

1. **Claim:** a bounded batch with `FOR UPDATE SKIP LOCKED` under a lease, oldest unpublished event per order only.
2. **Send:** with `acks=all` and producer idempotence, keyed by `orderId`, with no database transaction open.
3. **Mark:** published only after the broker confirms.

Delivery is **at least once**. A crash after the broker confirms and before the mark sends the same `eventId` again, and consumers dedupe on it. A crashed publisher's lease expires and another claims the row. Retries back off from 1 s to 5 min and never give up, because a committed event must not be lost.

## Consumers

`box1.inventory.events.v1` and `box1.fulfillment.events.v1` use consumer group `order`. Each event is one transaction: the `processed_event` row, the state change, history and outbox rows commit together or not at all. Offsets commit after processing.

| Failure | Handling |
|---|---|
| Malformed envelope, invalid payload, unknown type or unknown order | Straight to `box1.order.dlq.v1` |
| Database failure | Up to 20 attempts over about 7 minutes, holding the channel's position, then `box1.order.dlq.v1` |

Holding position keeps per-order order during ordinary outages. An outage longer than the retry budget moves events to the dead-letter topic, and consumption then moves past them. Replay is duplicate-safe but can apply an event after later ones, which is why a replayed reservation past its `expiresAt` moves the order to `EXCEPTION` rather than requesting fulfillment. A reservation whose `sku`, `quantity` or `siteId` differs from the order does the same.

## Kafka unavailable

Orders keep committing and the API keeps returning `201`. Readiness stays up, because Kafka is not a readiness dependency. `box1_outbox_pending` and `box1_outbox_oldest_pending_age_seconds` rise, and they are the outage signal.

## Security boundaries

- **No API authentication.** This is a temporary, deliberate position tracked in [box1-docs#40](https://github.com/box1-org/box1-docs/issues/40). `X-Consumer-Id` sets the idempotency scope, and is not an identity.
- The platform smoke task needs the Route and unauthenticated `/api/greetings`, `/api/info`, `/q/health/*`, `/q/openapi` and `/q/metrics`, so those stay.
- There are no internal or administrative HTTP endpoints. Replay is a runbook procedure.
- `customerId` appears only in `order.placed.v1`, as the contract requires, and in API responses. Logs never carry it, or `Idempotency-Key`, or `X-Consumer-Id`.
- The database password comes from a Secret reference, never from this repository.

## Telemetry

| Signal | Detail |
|---|---|
| Logs | JSON in the deployed image. MDC carries `orderId`, `eventId`, `eventType`, `correlationId`, `traceId` and `spanId`. |
| Traces | W3C `traceparent` is honored on HTTP, stored in envelopes, and continued from inbound events. Export is off until a collector exists (box1-docs#22). |
| Metrics | `box1_outbox_pending`, `box1_outbox_oldest_pending_age_seconds`, `box1_outbox_publish_lag_seconds`, `box1_outbox_published_total`, `box1_outbox_publish_failures_total`, `box1_orders_lifecycle_total{state}`, `box1_orders_stuck`, `box1_orders_oldest_open_age_seconds`, `box1_events_consumed_total{type,outcome}` |
