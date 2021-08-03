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
package io.arenadata.kafka.postgres.reader.configuration;

import io.arenadata.kafka.postgres.reader.configuration.properties.VertxProperties;
import io.arenadata.kafka.postgres.reader.utils.LoggerContextUtils;
import io.arenadata.kafka.postgres.reader.verticle.VerticleProducer;
import io.reactiverse.contextual.logging.ContextualData;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Slf4j
@Configuration
public class VertxConfiguration implements ApplicationListener<ApplicationReadyEvent> {
    private static final String DEPLOY_SUCCESS_MSG = "Verticle [{}] deployed";
    private static final String DEPLOY_ERROR_MSG = "Verticle [{}] failed to deploy";

    @Bean
    public WebClient webClient(Vertx vertx) {
        return WebClient.create(vertx);
    }

    @Bean
    @ConditionalOnMissingBean(Vertx.class)
    public Vertx vertx(VertxProperties vertxProperties) {
        VertxOptions options = new VertxOptions();
        options.setWorkerPoolSize(vertxProperties.getWorkersPoolSize());
        options.setEventLoopPoolSize(vertxProperties.getEventLoopPoolSize());
        Vertx vertx = Vertx.vertx(options);
        configureInterceptors(vertx);
        return vertx;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        Vertx vertx = event.getApplicationContext().getBean(Vertx.class);
        Map<String, VerticleProducer> verticles = event.getApplicationContext().getBeansOfType(VerticleProducer.class);
        log.debug("Verticles to deploy: {}", verticles.keySet());
        verticles.forEach((key, value) -> vertx.deployVerticle(value::produce, value.deploymentOptions(), stringAsyncResult -> {
            if (stringAsyncResult.succeeded()) {
                log.debug(DEPLOY_SUCCESS_MSG, key);
            } else {
                log.error(DEPLOY_ERROR_MSG, key, stringAsyncResult.cause());
                event.getApplicationContext().close();
            }
        }));
    }

    private void configureInterceptors(Vertx vertx) {
        vertx.eventBus().addOutboundInterceptor(event -> {
            String requestId = ContextualData.get("requestId");
            if (requestId != null) {
                event.message().headers().add("requestId", requestId);
            }
            event.next();
        });

        vertx.eventBus().addInboundInterceptor(event -> {
            String requestId = event.message().headers().get("requestId");
            if (requestId != null) {
                LoggerContextUtils.setRequestId(requestId);
            }
            event.next();
        });
    }
}
