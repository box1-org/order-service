package org.box1.order;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.order.domain.OrderState;
import org.box1.order.store.OutboxStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Business meters. Gauges read a cached snapshot, so a scrape never runs SQL. */
@ApplicationScoped
public class OrderMetrics {
    private static final Logger LOG = Logger.getLogger(OrderMetrics.class);

    private final MeterRegistry registry;
    private final OutboxStore outbox;
    private final double stuckAfterSeconds;
    private final AtomicReference<OutboxStore.Stats> stats =
            new AtomicReference<>(new OutboxStore.Stats(0, 0, 0, 0));
    private final AtomicLong refreshFailures = new AtomicLong();

    public OrderMetrics(MeterRegistry registry, OutboxStore outbox,
            @ConfigProperty(name = "order.stuck-after-seconds") double stuckAfterSeconds) {
        this.registry = registry;
        this.outbox = outbox;
        this.stuckAfterSeconds = stuckAfterSeconds;
        Gauge.builder("box1_outbox_pending", stats, s -> s.get().pending()).register(registry);
        Gauge.builder("box1_outbox_oldest_pending_age_seconds", stats, s -> s.get().oldestPendingSeconds()).register(registry);
        Gauge.builder("box1_orders_stuck", stats, s -> s.get().stuckOrders()).register(registry);
        Gauge.builder("box1_orders_oldest_open_age_seconds", stats, s -> s.get().oldestOpenOrderSeconds()).register(registry);
    }

    /** Refresh the snapshot. On failure the last values stay and a failure counter rises. */
    @Scheduled(every = "${order.metrics.refresh-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void refresh() {
        try {
            stats.set(outbox.stats(stuckAfterSeconds));
        } catch (RuntimeException error) {
            if (refreshFailures.getAndIncrement() == 0) {
                LOG.warn("Unable to refresh order gauges");
            }
            registry.counter("box1_order_metrics_refresh_failures_total").increment();
            return;
        }
        refreshFailures.set(0);
    }

    /** Count an order entering a state. */
    public void entered(OrderState state) {
        registry.counter("box1_orders_lifecycle", "state", state.name()).increment();
    }

    /** Count a consumed event by type and outcome. */
    public void consumed(String eventType, String outcome) {
        registry.counter("box1_events_consumed", "type", eventType, "outcome", outcome).increment();
    }

    /** Count a retry of an event after a transient failure. */
    public void retried() {
        registry.counter("box1_event_retries").increment();
    }

    /** Record the outbox lag of a confirmed publish and count the attempt. */
    public void published(double lagSeconds) {
        registry.counter("box1_outbox_published").increment();
        registry.summary("box1_outbox_publish_lag_seconds").record(lagSeconds);
    }

    /** Count a publish attempt the broker did not confirm. */
    public void publishFailed() {
        registry.counter("box1_outbox_publish_failures").increment();
    }
}
