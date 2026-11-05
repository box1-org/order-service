package org.box1.order.api;

import jakarta.ws.rs.core.Response;
import org.box1.order.OrderService;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Maps failures to RFC 9457 responses. Parser and driver messages are not returned to callers. */
public class ProblemMappers {
    private static final Logger LOG = Logger.getLogger(ProblemMappers.class);

    @ServerExceptionMapper
    public Response problem(ProblemException error) {
        return response(Problem.of(error.status(), error.title(), error.getMessage()));
    }

    @ServerExceptionMapper
    public Response conflict(OrderService.IdempotencyConflictException error) {
        return response(Problem.of(409, "Conflict", error.getMessage()));
    }

    @ServerExceptionMapper
    public Response store(org.box1.order.store.StoreException error) {
        LOG.error(error.getMessage());
        return response(Problem.of(503, "Service Unavailable", "The order store is unavailable"));
    }

    private static Response response(Problem problem) {
        return Response.status(problem.status()).type(Problem.MEDIA_TYPE).entity(problem).build();
    }
}
