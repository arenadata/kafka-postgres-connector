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
package io.arenadata.kafka.postgres.writer.verticle;

import io.arenadata.kafka.postgres.writer.configuration.AppConfiguration;
import io.arenadata.kafka.postgres.writer.controller.InsertDataController;
import io.arenadata.kafka.postgres.writer.controller.VersionController;
import io.arenadata.kafka.postgres.writer.model.DataTopic;
import io.arenadata.kafka.postgres.writer.verticle.handler.SendResponseHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class QueryVerticle extends AbstractVerticle {
    private final SendResponseHandler sendResponseHandler;
    private final InsertDataController insertDataController;
    private final VersionController versionController;
    private final AppConfiguration configuration;

    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);
        router.mountSubRouter("/", apiRouter());
        vertx.eventBus().consumer(DataTopic.SEND_RESPONSE.getValue(), this::handleSendResponse);
        vertx.createHttpServer().requestHandler(router).listen(configuration.httpPort())
                .onSuccess(httpServer -> {
                    log.info("Registered instance on port: [{}]", httpServer.actualPort());
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    private void handleSendResponse(Message<String> contextId) {
        sendResponseHandler.handleSendResponse(contextId.body());
    }

    private Router apiRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().consumes("application/json");
        router.route().produces("application/json");
        router.post("/newdata/start").handler(insertDataController::startLoad);
        router.post("/newdata/stop").handler(insertDataController::stopLoad);
        router.get("/versions").handler(versionController::version);
        return router;
    }
}
