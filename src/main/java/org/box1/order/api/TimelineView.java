package org.box1.order.api;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.box1.order.domain.OrderState;
import org.box1.order.domain.TimelineEntry;

/** Timeline entry as returned by the API. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TimelineView(
        long sequence,
        String kind,
        OrderState fromState,
        OrderState toState,
        String reasonCode,
        Instant occurredAt,
        UUID eventId,
        String eventType,
        String causationId,
        String correlationId,
        String traceId) {

    static TimelineView of(TimelineEntry entry) {
        return new TimelineView(entry.sequence(), entry.kind(), entry.fromState(), entry.toState(), entry.reasonCode(),
                entry.occurredAt(), entry.eventId(), entry.eventType(), entry.causationId(), entry.correlationId(),
                entry.traceId());
    }
}
