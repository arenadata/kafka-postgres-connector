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

import io.arenadata.kafka.postgres.reader.configuration.properties.PostgresProperties;
import io.arenadata.kafka.postgres.reader.service.PostgresExecutor;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import lombok.val;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class PostgresConfiguration {
    @Bean
    public List<PostgresExecutor> postgresExecutors(Vertx vertx,
                                                    PostgresProperties postgresProperties) {
        return Arrays.stream(postgresProperties.getHosts().split(","))
                .map(host -> {
                    val pgPool = prepareNewPool(host, postgresProperties, vertx);
                    return new PostgresExecutor(pgPool);
                })
                .collect(Collectors.toList());
    }

    private PgPool prepareNewPool(String hostPort, PostgresProperties postgresProperties, Vertx vertx) {
        val hostPortArr = hostPort.split(":");
        val host = hostPortArr[0];
        val port = Integer.parseInt(hostPortArr[1]);

        val pgConnectOptions = new PgConnectOptions();
        pgConnectOptions.setDatabase(postgresProperties.getDatabase());
        pgConnectOptions.setHost(host);
        pgConnectOptions.setPort(port);
        pgConnectOptions.setUser(postgresProperties.getUser());
        pgConnectOptions.setPassword(postgresProperties.getPassword());
        pgConnectOptions.setPreparedStatementCacheMaxSize(postgresProperties.getPreparedStatementsCacheMaxSize());
        pgConnectOptions.setPreparedStatementCacheSqlLimit(postgresProperties.getPreparedStatementsCacheSqlLimit());
        pgConnectOptions.setCachePreparedStatements(postgresProperties.isPreparedStatementsCache());
        pgConnectOptions.setPipeliningLimit(1);

        val poolOptions = new PoolOptions();
        poolOptions.setMaxSize(postgresProperties.getPoolSize());
        return PgPool.pool(vertx, pgConnectOptions, poolOptions);
    }
}
