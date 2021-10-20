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
package io.arenadata.kafka.postgres.reader.service;

import io.arenadata.kafka.postgres.reader.factory.QueryResultUpstreamFactory;
import io.arenadata.kafka.postgres.reader.model.QueryRequest;
import io.arenadata.kafka.postgres.reader.model.QueryResultItem;
import io.arenadata.kafka.postgres.reader.upstream.Upstream;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

@Slf4j
@Service
@AllArgsConstructor
public class PostgresQueryExecutorService {
    private final List<PostgresExecutor> databaseExecutors;
    private final QueryResultUpstreamFactory upstreamFactory;

    public Future<Void> execute(QueryRequest query) {
        return Future.future(p -> {
            val queryFutures = new ArrayList<Future>(databaseExecutors.size());
            IntStream.range(0, databaseExecutors.size()).forEach(i -> {
                log.info("Start executor [{}] for request: [{}]", i, query.getSql());
                final QueryRequest queryRequest = query.copy();
                queryRequest.setStreamTotal(databaseExecutors.size());
                queryRequest.setStreamNumber(i);
                queryFutures.add(executeQuery(databaseExecutors.get(i), queryRequest));
            });
            CompositeFuture.join(queryFutures)
                    .onSuccess(i -> p.complete())
                    .onFailure(p::fail);
        });
    }

    private Future<Void> executeQuery(PostgresExecutor executor,
                                      QueryRequest query) {
        return Future.future(p -> {
            val upstream = upstreamFactory.create(query.getAvroSchema(), query.getKafkaBrokers());
            val messageFutures = new CopyOnWriteArrayList<Future>();
            executor.execute(query, chunk -> messageFutures.add(pushMessage(query, upstream, chunk)))
                    .compose(v -> CompositeFuture.join(messageFutures))
                    .onComplete(ar -> {
                        upstream.close();
                        if (ar.succeeded()) {
                            p.complete();
                        } else {
                            p.fail(ar.cause());
                        }
                    });
        });
    }


    private Future<Void> pushMessage(QueryRequest query,
                                     Upstream<QueryResultItem> upstream,
                                     QueryResultItem resultItem) {
        return upstream.push(query, resultItem)
                .onSuccess(v -> log.debug("Chunk [{}] for table [{}] was pushed successfully into topic [{}] isLast [{}], stream number [{}]",
                        resultItem.getChunkNumber(), query.getTable(), query.getKafkaTopic(), resultItem.getIsLastChunk(), query.getStreamNumber()))
                .onFailure(t -> log.error("Error sending chunk [{}] for table [{}] into topic [{}] isLast [{}], stream number [{}]",
                        resultItem.getChunkNumber(), query.getTable(), query.getKafkaTopic(), resultItem.getIsLastChunk(), query.getStreamNumber(), t));
    }
}
