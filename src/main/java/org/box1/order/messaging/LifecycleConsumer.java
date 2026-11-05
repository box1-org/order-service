package org.box1.order.messaging;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.box1.order.OrderMetrics;
import org.box1.order.OrderService;
import org.box1.order.domain.LifecycleEvent;
import org.box1.order.domain.OrderState;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Consumes inventory and fulfillment events. A thrown exception nacks the record, and the connector's
 * dead-letter strategy moves it to the dead-letter topic. Offsets commit only after processing.
 */
@ApplicationScoped
public class LifecycleConsumer {
    private static final Logger LOG = Logger.getLogger(LifecycleConsumer.class);
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private final EventParser parser;
    private final OrderService orders;
    private final OrderMetrics metrics;
    private final Tracer tracer;
    private final int maxAttempts;
    private final long initialBackoffMillis;
    private final long maxBackoffMillis;

    public LifecycleConsumer(EventParser parser, OrderService orders, OrderMetrics metrics, Tracer tracer,
            @ConfigProperty(name = "order.consumer.max-attempts") int maxAttempts,
            @ConfigProperty(name = "order.consumer.initial-backoff-millis") long initialBackoffMillis,
            @ConfigProperty(name = "order.consumer.max-backoff-millis") long maxBackoffMillis) {
        this.parser = parser;
        this.orders = orders;
        this.metrics = metrics;
        this.tracer = tracer;
        this.maxAttempts = maxAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    @Incoming("inventory-events")
    @Blocking
    void onInventory(ConsumerRecord<String, byte[]> record) {
        process(text(record));
    }

    @Incoming("fulfillment-events")
    @Blocking
    void onFulfillment(ConsumerRecord<String, byte[]> record) {
        process(text(record));
    }

    /** Bytes stay raw on the channel, so a dead-lettered record is exactly what arrived. */
    private static String text(ConsumerRecord<String, byte[]> record) {
        return record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8);
    }

    private void process(String value) {
        LifecycleEvent event;
        try {
            event = parser.parse(value);
        } catch (PoisonEventException error) {
            metrics.consumed("unparsed", "dead_lettered");
            LOG.warnv("Dead-lettering malformed event: {0}", error.getMessage());
            throw error;
        }
        Context parent = extract(event.traceparent());
        SpanBuilder builder = tracer.spanBuilder("order.consume " + event.eventType()).setParent(parent);
        Span span = builder.startSpan();
        MDC.put("orderId", event.orderId());
        MDC.put("eventId", event.eventId().toString());
        MDC.put("eventType", event.eventType());
        MDC.put("correlationId", event.correlationId());
        try (Scope ignored = span.makeCurrent()) {
            OrderService.Applied applied = withRetries(orders::apply, event);
            applied.entered().forEach(metrics::entered);
            metrics.consumed(event.eventType(), applied.outcome());
            LOG.infov("Event {0}: states={1}", applied.outcome(), names(applied));
        } catch (RuntimeException error) {
            span.recordException(error);
            metrics.consumed(event.eventType(), "dead_lettered");
            LOG.warnv("Dead-lettering event: {0}", error.getClass().getSimpleName());
            if (error instanceof PoisonEventException) {
                throw error;
            }
            // Fixed text: this message and any cause reach dead-letter headers.
            throw new PoisonEventException("Retries exhausted");
        } finally {
            span.end();
            MDC.remove("orderId");
            MDC.remove("eventId");
            MDC.remove("eventType");
            MDC.remove("correlationId");
        }
    }

    private static String names(OrderService.Applied applied) {
        return applied.entered().stream().map(OrderState::name).toList().toString();
    }

    private <T, R> R withRetries(Function<T, R> action, T input) {
        long backoff = initialBackoffMillis;
        for (int attempt = 1; ; attempt++) {
            try {
                return action.apply(input);
            } catch (PoisonEventException error) {
                throw error;
            } catch (RuntimeException error) {
                if (attempt >= maxAttempts) {
                    throw error;
                }
                metrics.retried();
                LOG.warnv("Retrying event: attempt={0} error={1}", attempt, error.getClass().getSimpleName());
                sleep(backoff);
                backoff = Math.min(backoff * 2, maxBackoffMillis);
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during event retry", error);
        }
    }

    private static Context extract(String traceparent) {
        if (traceparent == null) {
            return Context.current();
        }
        return W3CTraceContextPropagator.getInstance().extract(Context.current(),
                Map.of("traceparent", traceparent), GETTER);
    }
}
