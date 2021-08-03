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

import io.arenadata.kafka.postgres.writer.model.VersionInfo;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@Component
public class VersionController {

    private final List<VersionInfo> versionInfos;

    public VersionController(BuildProperties buildProperties) {
        this.versionInfos = Collections.singletonList(new VersionInfo(buildProperties.getName(), buildProperties.getVersion()));
    }

    public void version(RoutingContext context) {
        context.response()
                .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON_VALUE)
                .setStatusCode(OK.code())
                .end(Json.encode(versionInfos));
    }
}
