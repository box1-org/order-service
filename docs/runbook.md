# Runbook

## Health

| Check | Meaning |
|---|---|
| `/q/health/live` | The process is running. Stays up during dependency outages. |
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
| `ORDER_CONSUMER_MAX_ATTEMPTS` | `5` | Attempts before dead-lettering |
| `ORDER_CONSUMER_INITIAL_BACKOFF_MILLIS` | `500` | First retry delay, then doubling |
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

**Signal:** readiness returns 503 and the Route stops sending traffic. Consumer events that exhaust their retries go to the dead-letter topic. After recovery, replay them as below.

## Dead-letter replay

**Not yet exercised on the cluster.** Local tests cover re-producing a dead-lettered record to its source topic and confirm it applies once. Cluster commands below have not been run, and `box1.order.dlq.v1` must exist first.

1. Record who is replaying, why, and the time. Keep that with the incident notes, because no automated audit record exists.
2. Read the dead-letter topic. The `dead-letter-topic` and `dead-letter-reason` headers give the source and cause:

   ```sh
   oc -n box1-kafka exec -it box1-kafka-broker-0 -- bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic box1.order.dlq.v1 --from-beginning \
     --property print.key=true --property print.headers=true --timeout-ms 10000
   ```

3. Fix the cause. A malformed record cannot be replayed until its producer corrects it.
4. Re-produce the value unchanged, with its original key, to the source topic. Replay is safe to repeat, because `processed_event` dedupes on `eventId`.
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

Measured values are recorded with the deployment. The node is a single shared host, so keep requests conservative.
