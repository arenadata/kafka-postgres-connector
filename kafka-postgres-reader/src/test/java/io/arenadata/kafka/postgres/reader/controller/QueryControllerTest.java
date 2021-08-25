package io.arenadata.kafka.postgres.reader.controller;

import io.arenadata.kafka.postgres.reader.model.QueryRequest;
import io.arenadata.kafka.postgres.reader.service.PostgresQueryExecutorService;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class QueryControllerTest {

    @Mock
    private PostgresQueryExecutorService queryExecutorService;

    @InjectMocks
    private QueryController queryController;

    @Mock
    private RoutingContext context;

    @Mock
    private HttpServerResponse response;

    @Mock
    private JsonObject bodyAsJson;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testErrorMessageIsSet() {
        when(context.getBodyAsJson()).thenReturn(bodyAsJson);
        when(bodyAsJson.mapTo(QueryRequest.class)).thenReturn(new QueryRequest());
        when(queryExecutorService.execute(any())).thenReturn(Future.failedFuture(new Exception("Exception message")));
        when(context.response()).thenReturn(response);

        queryController.query(context);
        verify(response).setStatusMessage("Exception message");
    }

}
