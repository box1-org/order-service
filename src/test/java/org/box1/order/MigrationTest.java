package org.box1.order;

import static org.box1.order.Harness.count;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ContainersTag
class MigrationTest {
    @Inject
    Flyway flyway;

    @Test
    void rerunAppliesNothingAndValidates() {
        assertEquals(0, flyway.migrate().migrationsExecuted);
        flyway.validate();
        assertEquals(0, flyway.info().pending().length);
    }

    @Test
    void allContractTablesExist() {
        for (String table : List.of("orders", "order_state_history", "idempotency_records", "outbox", "processed_event")) {
            assertEquals(1, count("SELECT count(*) FROM information_schema.tables WHERE table_name = ?", table), table);
        }
    }

    @Test
    void constraintsRejectBadRows() {
        String id = "ord_" + UUID.randomUUID().toString().replace("-", "");
        String insert = "INSERT INTO orders (order_id, customer_id, sku, quantity, site_id, state, status_reason, correlation_id, created_at, updated_at) "
                + "VALUES ('" + id + "', 'c', 's', %s, 'x', '%s', 'r', 'c', now(), now())";
        assertThrows(IllegalStateException.class, () -> Harness.exec(insert.formatted("0", "SUBMITTED")));
        assertThrows(IllegalStateException.class, () -> Harness.exec(insert.formatted("1", "NOT_A_STATE")));
        Harness.exec("BEGIN; " + insert.formatted("1", "SUBMITTED") + "; "
                + "INSERT INTO outbox (event_id, aggregate_id, event_type, payload, created_at, next_attempt_at) VALUES "
                + "(gen_random_uuid(), '" + id + "', 'order.placed.v1', '{}', now(), now() + interval '1 hour'); COMMIT");
        assertThrows(IllegalStateException.class, () -> Harness.exec(
                "INSERT INTO outbox (event_id, aggregate_id, event_type, payload, created_at, next_attempt_at) VALUES "
                + "(gen_random_uuid(), '" + id + "', 'order.placed.v1', '{}', now(), now())"));
    }

    @Test
    void lifecycleIndexesExist() throws SQLException {
        for (String index : List.of("ix_orders_open_updated", "ix_history_order_seq", "ix_outbox_unpublished", "ux_outbox_once_per_order")) {
            assertEquals(1, count("SELECT count(*) FROM pg_indexes WHERE indexname = ?", index), index);
        }
    }
}
