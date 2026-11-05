package org.box1.order.api;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.box1.order.OrderMetrics;
import org.box1.order.OrderService;
import org.box1.order.domain.Order;
import org.box1.order.domain.OrderState;
import org.box1.order.domain.TimelineEntry;
import org.box1.order.store.OrderStore;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** The order API from box1-ops-contracts openapi/order-service.yaml. */
@Path("/api/v1/orders")
@Produces(MediaType.APPLICATION_JSON)
public class OrdersResource {
    private static final Logger LOG = Logger.getLogger(OrdersResource.class);
    private static final Pattern CUSTOMER = Pattern.compile("^cust_[A-Za-z0-9_-]+$");
    private static final Pattern SITE = Pattern.compile("^site_[A-Za-z0-9_-]+$");
    private static final Pattern ORDER = Pattern.compile("^ord_[A-Za-z0-9_-]+$");
    private static final Pattern SCOPE = Pattern.compile("^[A-Za-z0-9_.-]{1,64}$");
    private static final Pattern CORRELATION = Pattern.compile("^[A-Za-z0-9_.:-]{1,128}$");
    private static final int MAX_KEY = 255;
    private static final int MAX_SKU = 80;
    private static final int DEFAULT_LIMIT = 20;
    private static final int MAX_LIMIT = 100;

    private final OrderService orders;
    private final OrderStore store;
    private final OrderMetrics metrics;

    public OrdersResource(OrderService orders, OrderStore store, OrderMetrics metrics) {
        this.orders = orders;
        this.store = store;
        this.metrics = metrics;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(operationId = "createOrder", summary = "Create an order, or return the original for a replayed key")
    public Response create(@HeaderParam("Idempotency-Key") String key, @HeaderParam("X-Consumer-Id") String consumer,
            @HeaderParam("X-Correlation-Id") String correlation, CreateOrderRequest body) {
        if (key == null || key.isEmpty() || key.length() > MAX_KEY) {
            throw badRequest("Idempotency-Key is required and must be 1 to 255 characters");
        }
        String scope = consumer == null ? "anonymous" : consumer;
        if (!SCOPE.matcher(scope).matches()) {
            throw badRequest("X-Consumer-Id is invalid");
        }
        validate(body);
        String correlationId = correlation != null && CORRELATION.matcher(correlation).matches()
                ? correlation : UUID.randomUUID().toString();
        OrderService.CreateResult result = orders.create(new OrderService.CreateCommand(scope, key, body.customerId(),
                body.sku(), body.quantity(), body.siteId(), correlationId));
        Order order = result.order();
        URI location = URI.create("/api/v1/orders/" + order.orderId());
        MDC.put("orderId", order.orderId());
        MDC.put("correlationId", order.correlationId());
        try {
            if (result.created()) {
                metrics.entered(OrderState.SUBMITTED);
                LOG.info("Order created");
                return Response.created(location).header("X-Correlation-Id", order.correlationId())
                        .entity(OrderView.of(order)).build();
            }
            LOG.info("Idempotent replay");
            return Response.ok(OrderView.of(order)).location(location).header("Idempotency-Replayed", "true")
                    .header("X-Correlation-Id", order.correlationId()).build();
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    @GET
    @Operation(operationId = "listOrders", summary = "List orders, newest first")
    public Response list(@QueryParam("limit") String limit, @QueryParam("cursor") String cursor) {
        int size = limit(limit);
        OrderStore.Page<Order> page = store.list(cursor == null ? null : decode(cursor), size);
        List<OrderView> items = page.items().stream().map(OrderView::of).toList();
        return paged(items, page.next(), "/api/v1/orders", size);
    }

    @GET
    @Path("/{orderId}")
    @Operation(operationId = "getOrder", summary = "Read one order")
    public OrderView get(@PathParam("orderId") String orderId) {
        return OrderView.of(existing(orderId));
    }

    @GET
    @Path("/{orderId}/timeline")
    @Operation(operationId = "getOrderTimeline", summary = "Read an order's immutable timeline, oldest first")
    public Response timeline(@PathParam("orderId") String orderId, @QueryParam("limit") String limit,
            @QueryParam("cursor") String cursor) {
        existing(orderId);
        int size = limit(limit);
        OrderStore.Page<TimelineEntry> page = store.timeline(orderId, cursor == null ? 0 : decode(cursor), size);
        List<TimelineView> items = page.items().stream().map(TimelineView::of).toList();
        return paged(items, page.next(), "/api/v1/orders/" + orderId + "/timeline", size);
    }

    /** Applies to this resource only: unreadable bodies and media types get Problem responses, without parser text. */
    @ServerExceptionMapper
    Response framework(WebApplicationException error) {
        int status = error.getResponse().getStatus();
        String title = error.getResponse().getStatusInfo().getReasonPhrase();
        String detail = status == 400 ? "Request body is not a valid order" : title;
        return Response.status(status).type(Problem.MEDIA_TYPE).entity(Problem.of(status, title, detail)).build();
    }

    /** Takes precedence over the framework's own JSON error body for binding failures. */
    @ServerExceptionMapper
    Response unreadable(JsonProcessingException error) {
        return Response.status(400).type(Problem.MEDIA_TYPE)
                .entity(Problem.of(400, "Bad Request", "Request body is not a valid order")).build();
    }

    /** The framework maps this subtype itself unless a mapper names it exactly. */
    @ServerExceptionMapper
    Response unreadable(MismatchedInputException error) {
        return unreadable((JsonProcessingException) error);
    }

    private Order existing(String orderId) {
        if (!ORDER.matcher(orderId).matches()) {
            throw new ProblemException(404, "Not Found", "Order not found");
        }
        return orders.find(orderId).orElseThrow(() -> new ProblemException(404, "Not Found", "Order not found"));
    }

    private static void validate(CreateOrderRequest body) {
        if (body == null) {
            throw badRequest("Request body is required");
        }
        if (body.customerId() == null || !CUSTOMER.matcher(body.customerId()).matches()) {
            throw badRequest("customerId is invalid");
        }
        if (body.sku() == null || body.sku().isEmpty() || body.sku().length() > MAX_SKU) {
            throw badRequest("sku is invalid");
        }
        if (body.quantity() == null || body.quantity() < 1) {
            throw badRequest("quantity must be greater than zero");
        }
        if (body.siteId() == null || !SITE.matcher(body.siteId()).matches()) {
            throw badRequest("siteId is invalid");
        }
    }

    private static int limit(String value) {
        if (value == null) {
            return DEFAULT_LIMIT;
        }
        try {
            int limit = Integer.parseInt(value);
            if (limit >= 1 && limit <= MAX_LIMIT) {
                return limit;
            }
        } catch (NumberFormatException error) {
            throw badRequest("limit must be between 1 and 100");
        }
        throw badRequest("limit must be between 1 and 100");
    }

    private static Response paged(Object items, Long next, String path, int size) {
        Response.ResponseBuilder response = Response.ok(items);
        if (next != null) {
            response.header("Link", "<" + path + "?limit=" + size + "&cursor=" + encode(next) + ">; rel=\"next\"");
        }
        return response.build();
    }

    private static String encode(long value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(Long.toString(value).getBytes(StandardCharsets.UTF_8));
    }

    private static long decode(String cursor) {
        try {
            if (cursor.length() > 64) {
                throw badRequest("cursor is invalid");
            }
            return Long.parseLong(new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException error) {
            throw badRequest("cursor is invalid");
        }
    }

    private static ProblemException badRequest(String detail) {
        return new ProblemException(400, "Bad Request", detail);
    }
}
