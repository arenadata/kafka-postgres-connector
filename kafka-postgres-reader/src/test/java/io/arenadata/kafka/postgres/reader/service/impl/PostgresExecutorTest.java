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
package io.arenadata.kafka.postgres.reader.service.impl;

import io.arenadata.kafka.postgres.reader.model.QueryRequest;
import io.arenadata.kafka.postgres.reader.model.QueryResultItem;
import io.arenadata.kafka.postgres.reader.service.PostgresExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PostgresExecutorTest {
    private static final String KAFKA_TOPIC = "kafkaTopic";
    private static final String SQL = "sql";

    @Mock
    private PgPool pgPool;

    @InjectMocks
    private PostgresExecutor postgresExecutor;

    @Mock
    private SqlConnection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private Row row;

    private final RowStreamStub streamStub = new RowStreamStub();

    @BeforeEach
    void setUp() {
        lenient().when(pgPool.withTransaction(any())).thenAnswer(invocationOnMock -> {
            Function<SqlConnection, Future<Void>> argument = invocationOnMock.getArgument(0, Function.class);
            return argument.apply(connection);
        });
        lenient().when(connection.prepare(anyString())).thenReturn(Future.succeededFuture(preparedStatement));
        lenient().when(preparedStatement.createStream(Mockito.anyInt())).thenReturn(streamStub);
        lenient().when(row.size()).thenReturn(3);
        lenient().when(row.getValue(anyInt())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
    }

    @Test
    void shouldSuccessWhenChunkSizeIsPerMessage() {
        // arrange
        val messages = new ArrayList<QueryResultItem>();

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setChunkSize(1);
        queryRequest.setKafkaTopic(KAFKA_TOPIC);
        queryRequest.setSql(SQL);

        // act execute
        Future<Void> result = postgresExecutor.execute(queryRequest, messages::add);

        // act handle x3
        assertFalse(result.isComplete());
        streamStub.rowHandler.handle(row);
        streamStub.rowHandler.handle(row);
        streamStub.rowHandler.handle(row);

        // act end handler
        assertFalse(result.isComplete());
        streamStub.endHandler.handle(null);

        // assert
        assertTrue(result.isComplete());
        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());

        assertThat(messages, contains(
                allOf(
                        hasProperty("chunkNumber", is(1)),
                        hasProperty("isLastChunk", is(false)),
                        hasProperty("dataSet", contains(contains(is(0), is(1), is(2))))
                ),
                allOf(
                        hasProperty("chunkNumber", is(2)),
                        hasProperty("isLastChunk", is(false)),
                        hasProperty("dataSet", contains(contains(is(0), is(1), is(2))))
                ),
                allOf(
                        hasProperty("chunkNumber", is(3)),
                        hasProperty("isLastChunk", is(false)),
                        hasProperty("dataSet", contains(contains(is(0), is(1), is(2))))
                ),
                allOf(
                        hasProperty("chunkNumber", is(4)),
                        hasProperty("isLastChunk", is(true)),
                        hasProperty("dataSet", empty())
                )
        ));
    }

    @Test
    void shouldSuccessWhenChunkedAndOneLeftBehind() {
        // arrange
        val messages = new ArrayList<QueryResultItem>();

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setChunkSize(2);
        queryRequest.setKafkaTopic(KAFKA_TOPIC);
        queryRequest.setSql(SQL);

        // act execute
        Future<Void> result = postgresExecutor.execute(queryRequest, messages::add);

        // act handle x3
        assertFalse(result.isComplete());
        streamStub.rowHandler.handle(row);
        streamStub.rowHandler.handle(row);
        streamStub.rowHandler.handle(row);
        streamStub.rowHandler.handle(row);
        streamStub.rowHandler.handle(row);

        // act end handler
        assertFalse(result.isComplete());
        streamStub.endHandler.handle(null);

        // assert
        assertTrue(result.isComplete());
        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());

        assertThat(messages, contains(
                allOf(
                        hasProperty("chunkNumber", is(1)),
                        hasProperty("isLastChunk", is(false)),
                        hasProperty("dataSet", contains(
                                contains(is(0), is(1), is(2)),
                                contains(is(0), is(1), is(2))
                        ))
                ),
                allOf(
                        hasProperty("chunkNumber", is(2)),
                        hasProperty("isLastChunk", is(false)),
                        hasProperty("dataSet", contains(
                                contains(is(0), is(1), is(2)),
                                contains(is(0), is(1), is(2))
                        ))
                ),
                allOf(
                        hasProperty("chunkNumber", is(3)),
                        hasProperty("isLastChunk", is(true)),
                        hasProperty("dataSet", contains(
                                contains(is(0), is(1), is(2))
                        ))
                )
        ));
    }

    @Test
    void shouldFailWhenTransactionFailed() {
        // arrange
        val messages = new ArrayList<QueryResultItem>();

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setChunkSize(2);
        queryRequest.setKafkaTopic(KAFKA_TOPIC);
        queryRequest.setSql(SQL);

        reset(pgPool);
        when(pgPool.withTransaction(any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act execute
        Future<Void> result = postgresExecutor.execute(queryRequest, messages::add);

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("Unexpected success"));
        }
        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

    @Test
    void shouldFailWhenPrepareStatementFailed() {
        // arrange
        val messages = new ArrayList<QueryResultItem>();

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setChunkSize(2);
        queryRequest.setKafkaTopic(KAFKA_TOPIC);
        queryRequest.setSql(SQL);

        reset(connection);
        when(connection.prepare(anyString())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act execute
        Future<Void> result = postgresExecutor.execute(queryRequest, messages::add);

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("Unexpected success"));
        }
        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

    @Test
    void shouldFailWhenFailAfterHandle() {
        // arrange
        val messages = new ArrayList<QueryResultItem>();

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setChunkSize(2);
        queryRequest.setKafkaTopic(KAFKA_TOPIC);
        queryRequest.setSql(SQL);

        // act execute
        Future<Void> result = postgresExecutor.execute(queryRequest, messages::add);

        // act handle and fail
        assertFalse(result.isComplete());
        streamStub.rowHandler.handle(row);

        // act end handler
        assertFalse(result.isComplete());
        streamStub.exceptionHandler.handle(new RuntimeException("Exception"));

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("Unexpected success"));
        }

        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

    static class RowStreamStub implements RowStream<Row> {
        Handler<Row> rowHandler;
        Handler<Void> endHandler;
        Handler<Throwable> exceptionHandler;

        @Override
        public RowStream<Row> exceptionHandler(Handler<Throwable> handler) {
            exceptionHandler = handler;
            return this;
        }

        @Override
        public RowStream<Row> handler(Handler<Row> handler) {
            this.rowHandler = handler;
            return this;
        }

        @Override
        public RowStream<Row> pause() {
            return this;
        }

        @Override
        public RowStream<Row> resume() {
            return this;
        }

        @Override
        public RowStream<Row> endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;
        }

        @Override
        public RowStream<Row> fetch(long l) {
            return this;
        }

        @Override
        public Future<Void> close() {
            return null;
        }

        @Override
        public void close(Handler<AsyncResult<Void>> completionHandler) {

        }
    }
}