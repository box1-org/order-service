CREATE TABLE orders (
    order_seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    order_id text PRIMARY KEY,
    customer_id text NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    site_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('SUBMITTED','RESERVED','FULFILLMENT_REQUESTED','SHIPPED','COMPLETED','REJECTED','EXCEPTION','CANCELLED')),
    status_reason text NOT NULL,
    reservation_id text NULL,
    fulfillment_request_id text NULL UNIQUE,
    correlation_id text NOT NULL,
    trace_id text NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE INDEX ix_orders_open_updated ON orders (updated_at)
    WHERE state IN ('SUBMITTED','RESERVED','FULFILLMENT_REQUESTED');
CREATE INDEX ix_orders_state ON orders (state);

CREATE TABLE order_state_history (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id text NOT NULL REFERENCES orders (order_id),
    kind text NOT NULL CHECK (kind IN ('TRANSITION','NOTE')),
    from_state text NULL,
    to_state text NULL,
    reason_code text NOT NULL,
    occurred_at timestamptz NOT NULL,
    event_id uuid NULL,
    event_type text NULL,
    causation_id text NULL,
    correlation_id text NULL,
    trace_id text NULL
);
CREATE INDEX ix_history_order_seq ON order_state_history (order_id, seq);

CREATE FUNCTION reject_history_change() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'order_state_history is immutable';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER order_state_history_immutable
    BEFORE UPDATE OR DELETE ON order_state_history
    FOR EACH ROW EXECUTE FUNCTION reject_history_change();
CREATE TRIGGER order_state_history_no_truncate
    BEFORE TRUNCATE ON order_state_history
    FOR EACH STATEMENT EXECUTE FUNCTION reject_history_change();

-- The order row follows its idempotency record in the same transaction.
CREATE TABLE idempotency_records (
    consumer_scope text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    order_id text NOT NULL REFERENCES orders (order_id) DEFERRABLE INITIALLY DEFERRED,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (consumer_scope, idempotency_key)
);

CREATE TABLE outbox (
    seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_id uuid PRIMARY KEY,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    published_at timestamptz NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    claimed_by text NULL,
    claim_expires_at timestamptz NULL,
    last_error text NULL
);
CREATE INDEX ix_outbox_unpublished ON outbox (next_attempt_at, seq) WHERE published_at IS NULL;
CREATE INDEX ix_outbox_aggregate_unpublished ON outbox (aggregate_id, seq) WHERE published_at IS NULL;
-- Database-level guard: these events are emitted at most once per order.
CREATE UNIQUE INDEX ux_outbox_once_per_order ON outbox (aggregate_id, event_type)
    WHERE event_type IN ('order.placed.v1', 'fulfillment.requested.v1');

CREATE TABLE processed_event (
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL,
    processed_at timestamptz NOT NULL
);
