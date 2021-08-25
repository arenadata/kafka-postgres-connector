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
import io.arenadata.kafka.postgres.reader.utils.LoggerContextUtils;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@Slf4j
@Component
public class QueryController {
    private final PostgresQueryExecutorService queryExecutorService;

    @Autowired
    public QueryController(PostgresQueryExecutorService queryExecutorService) {
        this.queryExecutorService = queryExecutorService;
    }

    public void query(RoutingContext context) {
        try {
            val query = context.getBodyAsJson().mapTo(QueryRequest.class);
            LoggerContextUtils.setRequestId(UUID.randomUUID());
            log.info("Request[{}]: [{}]", context.normalizedPath(), query);

            queryExecutorService.execute(query)
                    .onSuccess(v -> {
                        log.info("Query execute SUCCESS");
                        context.response()
                                .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON_VALUE)
                                .setStatusCode(OK.code())
                                .end();
                    })
                    .onFailure(t -> {
                        log.error("Query execute FAILED", t);
                        context.response().setStatusMessage(t.getMessage());
                        context.fail(t);
                    });
        } catch (Exception e) {
            log.error("Exception during request prepare, request: {}", context.getBodyAsString(), e);
            context.fail(e);
        }
    }
}
