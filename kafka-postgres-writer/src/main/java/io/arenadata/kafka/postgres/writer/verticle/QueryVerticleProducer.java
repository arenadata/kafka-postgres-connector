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
import io.arenadata.kafka.postgres.writer.configuration.properties.VerticleProperties;
import io.arenadata.kafka.postgres.writer.controller.InsertDataController;
import io.arenadata.kafka.postgres.writer.controller.VersionController;
import io.arenadata.kafka.postgres.writer.verticle.handler.SendResponseHandler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class QueryVerticleProducer implements VerticleProducer {
    private final SendResponseHandler sendResponseHandler;
    private final InsertDataController insertDataController;
    private final VersionController versionController;
    private final AppConfiguration configuration;
    private final VerticleProperties verticleProperties;

    @Override
    public Verticle produce() {
        return new QueryVerticle(sendResponseHandler, insertDataController, versionController, configuration);
    }

    @Override
    public DeploymentOptions deploymentOptions() {
        return new DeploymentOptions()
                .setInstances(verticleProperties.getQuery().getInstances());
    }
}
