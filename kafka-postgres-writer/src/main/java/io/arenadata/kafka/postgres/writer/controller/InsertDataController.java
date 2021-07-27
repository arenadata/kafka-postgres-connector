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
package io.arenadata.kafka.postgres.writer.controller;

import io.arenadata.kafka.postgres.writer.model.DataTopic;
import io.arenadata.kafka.postgres.writer.model.InsertDataContext;
import io.arenadata.kafka.postgres.writer.model.InsertDataRequest;
import io.arenadata.kafka.postgres.writer.model.StopRequest;
import io.arenadata.kafka.postgres.writer.repository.InsertDataContextRepository;
import io.arenadata.kafka.postgres.writer.service.executor.InsertDataRequestExecutor;
import io.arenadata.kafka.postgres.writer.utils.LoggerContextUtils;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

@Slf4j
@Component
@RequiredArgsConstructor
public class InsertDataController {
    private final InsertDataContextRepository dataContextRepository;
    private final InsertDataRequestExecutor executor;
    private final Vertx vertx;

    public void startLoad(RoutingContext context) {
        try {
            val request = context.getBodyAsJson().mapTo(InsertDataRequest.class);
            LoggerContextUtils.setRequestId(request.getRequestId());
            log.info("Request[{}]: [{}]", context.normalizedPath(), request);
            val insertDataContext = dataContextRepository.get(InsertDataContext.createContextId(request.getKafkaTopic(), request.getRequestId()))
                    .orElseGet(() -> new InsertDataContext(request, context));
            insertDataContext.setContext(context);
            executor.execute(insertDataContext);
        } catch (Exception e) {
            log.error("Exception during request prepare, request: {}", context.getBodyAsString(), e);
            context.fail(e);
        }
    }

    public void stopLoad(RoutingContext ctx) {
        try {
            val request = ctx.getBodyAsJson().mapTo(StopRequest.class);
            LoggerContextUtils.setRequestId(request.getRequestId());
            log.info("Request[{}]: [{}]", ctx.normalizedPath(), request);
            String contextId = InsertDataContext.createContextId(request.getKafkaTopic(), request.getRequestId());
            dataContextRepository.get(contextId)
                    .ifPresent(insertDataContext -> vertx.eventBus().send(DataTopic.SEND_RESPONSE.getValue(), contextId));
            ctx.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(200)
                    .end("");
        } catch (Exception e) {
            log.error("Exception during request prepare, request: {}", ctx.getBodyAsString(), e);
            ctx.fail(e);
        }
    }

}
