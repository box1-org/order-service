# Runbook

## Health

| Check | Meaning |
|---|---|
| `/q/health/live` | The process is running. Stays up during broker and database outages. Goes down if the dead-letter topic cannot accept a poison record, because the channel stops. The pod then restarts and the record redelivers until the topic is available. |
| `/q/health/ready` | PostgreSQL is reachable. Kafka is deliberately excluded. |

## Configuration

Set in `workloads/order-service/` in `box1-workloads-dev`.

| Variable | Default | Purpose |
|---|---|---|
| `QUARKUS_DATASOURCE_JDBC_URL`, `QUARKUS_DATASOURCE_USERNAME` | none | Database location and login |
| `QUARKUS_DATASOURCE_PASSWORD` | none | Secret reference only |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address. Set this in every deployment. |
| `ORDER_COMPLETION_POLICY` | `none` | `none` or `on-shipment` |
| `ORDER_OUTBOX_POLL_INTERVAL` | `1s` | Publisher poll |
| `ORDER_OUTBOX_BATCH_SIZE` | `100` | Maximum claimed per poll |
| `ORDER_OUTBOX_LEASE_SECONDS` | `60` | Claim lease |
| `ORDER_OUTBOX_BACKOFF_BASE_SECONDS`, `ORDER_OUTBOX_BACKOFF_MAX_SECONDS` | `1`, `300` | Publish retry backoff |
| `ORDER_CONSUMER_MAX_ATTEMPTS` | `20` | Attempts before dead-lettering |
| `ORDER_CONSUMER_INITIAL_BACKOFF_MILLIS`, `ORDER_CONSUMER_MAX_BACKOFF_MILLIS` | `500`, `30000` | Retry delay, doubling up to the cap |
| `ORDER_STUCK_AFTER_SECONDS` | `900` | Age before an open order counts as stuck |

## Broker outage

**Signal:** `box1_outbox_pending` rises, and the log shows `Outbox publish not confirmed`.

No action is needed for data safety. Orders keep committing, and the publisher catches up after the broker recovers, within the backoff cap of 5 minutes. Confirm with:

```sh
curl -s https://order-service-dev.apps.ocp.box1.org/q/metrics | grep box1_outbox
```

## Pod crash

After a crash, consumption resumes when the broker expires the dead member's session, about 45 seconds by the Kafka default. Publishing resumes when the claim lease expires, 60 seconds by default. Uncommitted work rolls back and is redelivered. This delay is expected.

## Database outage

**Signal:** readiness returns 503 and the Route stops sending traffic. `box1_event_retries_total` rises, and gauges keep their last values while `box1_order_metrics_refresh_failures_total` rises.

Consumers hold position and retry for about 7 minutes. Beyond that, events go to the dead-letter topic, so replay them as below after recovery. If the pod restarts while the database is down, Flyway fails at startup and the pod crash-loops until the database returns.

## Dead-letter replay

**Not yet exercised on the cluster.** Local tests cover re-producing a dead-lettered record to its source topic and confirm it applies once. The console commands below were tried only against a local broker. `box1.order.dlq.v1` must exist before this service deploys: otherwise the broker may auto-create it with default retention.

1. Record who is replaying, why, and the time. Keep that with the incident notes, because no automated audit record exists.
2. Read the dead-letter topic. The `dead-letter-topic` and `dead-letter-reason` headers give the source and cause:

   ```sh
   oc -n box1-kafka exec box1-kafka-box1-kafka-0 -- bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic box1.order.dlq.v1 --from-beginning \
     --property print.key=true --property print.headers=true --timeout-ms 10000
   ```

3. Fix the cause. A malformed record cannot be replayed until its producer corrects it. Check replay order: replay an order's events oldest first, and read its timeline first, because later events may have been applied already.
4. Save one record as a single line, `<key><TAB><value>`, in a file outside any repository, then re-produce it unchanged to its source topic. Do not pipe the whole topic back. Replay is safe to repeat, because `processed_event` dedupes on `eventId`:

   ```sh
   oc -n box1-kafka exec -i box1-kafka-box1-kafka-0 -- bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 --topic <source-topic> \
     --property parse.key=true --property "key.separator=$(printf '\t')" < record.tsv
   ```

5. Check the order's timeline.

Dead-letter records keep original values, so treat the topic as sensitive as its source. The topic has no delete tool here; records age out after 14 days.

## Stuck orders

`box1_orders_stuck` counts orders in an open state past the threshold. Read `/api/v1/orders/{orderId}/timeline`, then search logs for that `orderId`. Because this service has no cluster trace backend yet, use `traceId` from the timeline to correlate logs only.

`EXCEPTION` orders need manual reconciliation. No tooling for that exists yet.

## Migrations and rollback

- Flyway applies `src/main/resources/db/migration/` at startup, before readiness.
- Applied migrations are immutable. Roll back forward, with a corrective migration.
- Back up with `pg_dump --format=custom` as in `postgres/STORAGE-BACKUP-RESTORE.md` in `box1-ops-contracts`. Restore into a separate database first.
- Image rollback: revert the promotion commit in `box1-workloads-dev`. V1 stays applied, and the scaffold image ignores those tables.

## Resources

| Container | Request | Limit | Basis |
|---|---|---|---|
| `application` | 100m, 256Mi | 1 CPU, 512Mi | About 276 MiB RSS with a 128 MiB heap at 5 POST/s for 60 s on a workstation. The JVM heap is capped at 50% of the limit. Not yet measured on the cluster. |
| `postgres` | 50m, 128Mi | 1 CPU, 512Mi | Scaffold pod uses about 21 MiB idle. Not measured under load. |

The node is a single shared host, so keep requests conservative.
