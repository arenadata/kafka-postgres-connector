/*
 * Copyright © 2021 Arenadata Software LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
