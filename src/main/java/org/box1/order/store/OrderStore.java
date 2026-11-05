package org.box1.order.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import org.box1.order.domain.Order;
import org.box1.order.domain.OrderState;
import org.box1.order.domain.TimelineEntry;

/** SQL for orders, history, idempotency records and processed events. Callers own the transaction. */
@ApplicationScoped
public class OrderStore {
    private static final String ORDER_COLUMNS = "order_id, customer_id, sku, quantity, site_id, state, status_reason,"
            + " reservation_id, fulfillment_request_id, correlation_id, trace_id, created_at, updated_at";
    private static final String SELECT_ORDER = "SELECT " + ORDER_COLUMNS + " FROM orders WHERE order_id = ?";
    private static final String LOCK_ORDER = SELECT_ORDER + " FOR UPDATE";
    private static final String LIST_FIRST = "SELECT order_seq, " + ORDER_COLUMNS
            + " FROM orders ORDER BY order_seq DESC LIMIT ?";
    private static final String LIST_AFTER = "SELECT order_seq, " + ORDER_COLUMNS
            + " FROM orders WHERE order_seq < ? ORDER BY order_seq DESC LIMIT ?";
    private static final String TIMELINE = "SELECT seq, kind, from_state, to_state, reason_code, occurred_at, event_id,"
            + " event_type, causation_id, correlation_id, trace_id FROM order_state_history"
            + " WHERE order_id = ? AND seq > ? ORDER BY seq LIMIT ?";

    private final DataSource dataSource;

    public OrderStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Insert the idempotency record. False means another request holds this key. Blocks until that request ends. */
    public boolean claimKey(String scope, String key, String hash, String orderId, Instant now) {
        String sql = "INSERT INTO idempotency_records (consumer_scope, idempotency_key, request_hash, order_id, created_at)"
                + " VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, scope);
            s.setString(2, key);
            s.setString(3, hash);
            s.setString(4, orderId);
            s.setObject(5, at(now));
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Read a committed idempotency record. */
    public Optional<KeyRecord> findKey(String scope, String key) {
        String sql = "SELECT request_hash, order_id FROM idempotency_records WHERE consumer_scope = ? AND idempotency_key = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, scope);
            s.setString(2, key);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(new KeyRecord(r.getString(1), r.getString(2))) : Optional.empty();
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Insert a new SUBMITTED order. */
    public void insertOrder(Order order) {
        String sql = "INSERT INTO orders (" + ORDER_COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, order.orderId());
            s.setString(2, order.customerId());
            s.setString(3, order.sku());
            s.setInt(4, order.quantity());
            s.setString(5, order.siteId());
            s.setString(6, order.state().name());
            s.setString(7, order.statusReason());
            s.setString(8, order.reservationId());
            s.setString(9, order.fulfillmentRequestId());
            s.setString(10, order.correlationId());
            s.setString(11, order.traceId());
            s.setObject(12, at(order.createdAt()));
            s.setObject(13, at(order.updatedAt()));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Read an order without locking. */
    public Optional<Order> find(String orderId) {
        return readOne(SELECT_ORDER, orderId);
    }

    /** Read and lock an order for the rest of the transaction. */
    public Optional<Order> lock(String orderId) {
        return readOne(LOCK_ORDER, orderId);
    }

    private Optional<Order> readOne(String sql, String orderId) {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, orderId);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(order(r, 1)) : Optional.empty();
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Newest first with keyset pagination, so inserts do not shift later pages. */
    public Page<Order> list(Long before, int limit) {
        String sql = before == null ? LIST_FIRST : LIST_AFTER;
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            int index = 1;
            if (before != null) {
                s.setLong(index++, before);
            }
            s.setInt(index, limit + 1);
            List<Order> items = new ArrayList<>();
            Long next = null;
            try (ResultSet r = s.executeQuery()) {
                long last = 0;
                while (r.next()) {
                    if (items.size() == limit) {
                        next = last;
                        break;
                    }
                    last = r.getLong(1);
                    items.add(order(r, 2));
                }
            }
            return new Page<>(items, next);
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Timeline entries in insertion order. */
    public Page<TimelineEntry> timeline(String orderId, long after, int limit) {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(TIMELINE)) {
            s.setString(1, orderId);
            s.setLong(2, after);
            s.setInt(3, limit + 1);
            List<TimelineEntry> items = new ArrayList<>();
            Long next = null;
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    if (items.size() == limit) {
                        next = items.get(limit - 1).sequence();
                        break;
                    }
                    items.add(entry(r));
                }
            }
            return new Page<>(items, next);
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Move an order's state. The caller holds the row lock. */
    public void updateState(String orderId, OrderState state, String reason, String reservationId,
            String fulfillmentRequestId, Instant now) {
        String sql = "UPDATE orders SET state = ?, status_reason = ?, reservation_id = COALESCE(?, reservation_id),"
                + " fulfillment_request_id = COALESCE(?, fulfillment_request_id), updated_at = ? WHERE order_id = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, state.name());
            s.setString(2, reason);
            s.setString(3, reservationId);
            s.setString(4, fulfillmentRequestId);
            s.setObject(5, at(now));
            s.setString(6, orderId);
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Append an immutable history row. */
    public void appendHistory(String orderId, String kind, OrderState from, OrderState to, String reason, Instant at,
            UUID eventId, String eventType, String causationId, String correlationId, String traceId) {
        String sql = "INSERT INTO order_state_history (order_id, kind, from_state, to_state, reason_code, occurred_at,"
                + " event_id, event_type, causation_id, correlation_id, trace_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, orderId);
            s.setString(2, kind);
            s.setString(3, from == null ? null : from.name());
            s.setString(4, to == null ? null : to.name());
            s.setString(5, reason);
            s.setObject(6, at(at));
            s.setObject(7, eventId);
            s.setString(8, eventType);
            s.setString(9, causationId);
            s.setString(10, correlationId);
            s.setString(11, traceId);
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Record an event as processed. False means it was already processed. */
    public boolean markProcessed(UUID eventId, String eventType, Instant now) {
        String sql = "INSERT INTO processed_event (event_id, event_type, processed_at) VALUES (?, ?, ?) ON CONFLICT DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setObject(1, eventId);
            s.setString(2, eventType);
            s.setObject(3, at(now));
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    static OffsetDateTime at(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static Order order(ResultSet r, int first) throws SQLException {
        return new Order(r.getString(first), r.getString(first + 1), r.getString(first + 2), r.getInt(first + 3),
                r.getString(first + 4), OrderState.valueOf(r.getString(first + 5)), r.getString(first + 6),
                r.getString(first + 7), r.getString(first + 8), r.getString(first + 9), r.getString(first + 10),
                r.getObject(first + 11, OffsetDateTime.class).toInstant(),
                r.getObject(first + 12, OffsetDateTime.class).toInstant());
    }

    private static TimelineEntry entry(ResultSet r) throws SQLException {
        String from = r.getString(3);
        String to = r.getString(4);
        return new TimelineEntry(r.getLong(1), r.getString(2), from == null ? null : OrderState.valueOf(from),
                to == null ? null : OrderState.valueOf(to), r.getString(5),
                r.getObject(6, OffsetDateTime.class).toInstant(), r.getObject(7, UUID.class), r.getString(8),
                r.getString(9), r.getString(10), r.getString(11));
    }

    /** The stored fingerprint and order for an idempotency key. */
    public record KeyRecord(String requestHash, String orderId) {}

    /** One page of results and the cursor for the next, or null at the end. */
    public record Page<T>(List<T> items, Long next) {
        public Page {
            items = List.copyOf(items);
        }
    }
}
