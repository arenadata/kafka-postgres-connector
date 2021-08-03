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
package io.arenadata.kafka.postgres.writer.service.executor.impl;

import io.arenadata.kafka.postgres.writer.model.kafka.InsertChunk;
import io.arenadata.kafka.postgres.writer.model.sql.PostgresInsertSqlRequest;
import io.arenadata.kafka.postgres.writer.service.executor.PostgresExecutor;
import io.vertx.core.Future;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PostgresExecutorTest {

    @Mock
    private PgPool pgPool;

    @Mock
    private SqlConnection connection;

    @Mock
    private PreparedQuery<RowSet<Row>> preparedQuery;

    @Mock
    private RowSet<Row> rowSet;

    @InjectMocks
    private PostgresExecutor postgresExecutor;

    @BeforeEach
    void setUp() {
        lenient().when(pgPool.withConnection(any())).thenAnswer(invocationOnMock -> {
            Function<SqlConnection, Future<Void>> argument = invocationOnMock.getArgument(0, Function.class);
            return argument.apply(connection);
        });
        lenient().when(connection.preparedQuery(anyString())).thenReturn(preparedQuery);
        lenient().when(preparedQuery.executeBatch(any())).thenReturn(Future.succeededFuture(rowSet));
        lenient().when(rowSet.size()).thenReturn(100);
    }

    @Test
    void shouldSuccessWhenEverythingOk() {
        // arrange
        InsertChunk chunk = new InsertChunk();
        chunk.setInsertSqlRequest(new PostgresInsertSqlRequest("sql", Collections.emptyList()));

        // act
        Future<Integer> result = postgresExecutor.processChunk(chunk);

        // assert
        assertTrue(result.isComplete());
        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());
        assertEquals(100, result.result());
    }

    @Test
    void shouldFailWhenWithConnectionFailed() {
        // arrange
        InsertChunk chunk = new InsertChunk();
        chunk.setInsertSqlRequest(new PostgresInsertSqlRequest("sql", Collections.emptyList()));

        reset(pgPool);
        when(pgPool.withConnection(Mockito.any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        Future<Integer> result = postgresExecutor.processChunk(chunk);

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("UnexpectedSuccess"));
        }
        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

    @Test
    void shouldFailWhenExecuteFailed() {
        // arrange
        InsertChunk chunk = new InsertChunk();
        chunk.setInsertSqlRequest(new PostgresInsertSqlRequest("sql", Collections.emptyList()));

        reset(preparedQuery);
        when(preparedQuery.executeBatch(Mockito.any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        Future<Integer> result = postgresExecutor.processChunk(chunk);

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("UnexpectedSuccess"));
        }
        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

}