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

import io.arenadata.kafka.postgres.reader.model.QueryRequest;
import io.arenadata.kafka.postgres.reader.model.QueryResultItem;
import io.vertx.core.Future;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
@AllArgsConstructor
public class PostgresExecutor {
    private final PgPool pgPool;

    public Future<Void> execute(QueryRequest query, Consumer<QueryResultItem> chunkHandler) {
        return pgPool.withTransaction(sqlConnection -> receiveChunks(sqlConnection, query, chunkHandler));
    }

    private Future<Void> receiveChunks(SqlConnection connection,
                                       QueryRequest query,
                                       Consumer<QueryResultItem> chunkHandler) {
        return Future.future(p -> {
            log.info("Start query execution [{}], stream number: [{}]", query.getSql(), query.getStreamNumber());
            connection.prepare(query.getSql())
                    .onSuccess(preparedStatement -> {
                        log.debug("Query prepared successfully, stream number: [{}]", query.getStreamNumber());
                        val chunkNumber = new AtomicInteger(1);
                        val dataSet = new ArrayList<List<?>>();
                        val stream = preparedStatement.createStream(query.getChunkSize());
                        stream.handler(row -> {
                            val rowData = new ArrayList<>();
                            for (int i = 0; i < row.size(); i++) {
                                rowData.add(row.getValue(i));
                            }
                            dataSet.add(rowData);
                            if (dataSet.size() == query.getChunkSize()) {
                                log.debug("Stream read chunk [{}] filled, stream number: [{}]", chunkNumber.get(), query.getStreamNumber());
                                chunkHandler.accept(new QueryResultItem(
                                        new ArrayList<>(dataSet),
                                        chunkNumber.getAndIncrement(),
                                        false));
                                dataSet.clear();
                            }
                        }).endHandler(v -> {
                            log.info("Stream read success, last chunk: [{}], stream number: [{}]", chunkNumber.get(), query.getStreamNumber());
                            chunkHandler.accept(new QueryResultItem(
                                    new ArrayList<>(dataSet),
                                    chunkNumber.getAndIncrement(),
                                    true));
                            dataSet.clear();
                            stream.close();
                            p.complete();
                        }).exceptionHandler(t -> {
                            log.error("Exception during stream read, stream number: [{}]", query.getStreamNumber(), t);
                            stream.close();
                            p.fail(t);
                        });
                    })
                    .onFailure(t -> {
                        log.error("Exception during query prepare, stream number: [{}]", query.getStreamNumber(), t);
                        p.fail(t);
                    });
        });
    }
}
