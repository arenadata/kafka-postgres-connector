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

import io.arenadata.kafka.postgres.reader.factory.QueryResultUpstreamFactory;
import io.arenadata.kafka.postgres.reader.model.QueryRequest;
import io.arenadata.kafka.postgres.reader.model.QueryResultItem;
import io.arenadata.kafka.postgres.reader.service.PostgresExecutor;
import io.arenadata.kafka.postgres.reader.service.PostgresQueryExecutorService;
import io.arenadata.kafka.postgres.reader.upstream.Upstream;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostgresQueryExecutorServiceTest {

    @Mock
    private PostgresExecutor databaseExecutors1;

    @Mock
    private PostgresExecutor databaseExecutors2;

    @Mock
    private QueryResultUpstreamFactory upstreamFactory;

    @Mock
    private Upstream<QueryResultItem> upstream;

    private PostgresQueryExecutorService queryExecutorService;

    @BeforeEach
    void setUp() {
        queryExecutorService = new PostgresQueryExecutorService(Arrays.asList(databaseExecutors1, databaseExecutors2), upstreamFactory);
        when(upstreamFactory.create(any(), any())).thenReturn(upstream);
    }

    @Test
    void shouldSuccessWhenAllExecutorsAndMessagesSuccess() {
        // arrange
        when(databaseExecutors1.execute(any(), any())).thenAnswer(invocationOnMock -> {
            Consumer<QueryResultItem> argument = invocationOnMock.getArgument(1, Consumer.class);
            argument.accept(new QueryResultItem(null, 1, true));
            return Future.succeededFuture();
        });
        when(databaseExecutors2.execute(any(), any())).thenAnswer(invocationOnMock -> {
            Consumer<QueryResultItem> argument = invocationOnMock.getArgument(1, Consumer.class);
            argument.accept(new QueryResultItem(null, 1, true));
            return Future.succeededFuture();
        });
        when(upstream.push(Mockito.any(), Mockito.any())).thenReturn(Future.succeededFuture());


        // act
        Future<Void> result = queryExecutorService.execute(new QueryRequest());

        // assert
        assertTrue(result.isComplete());

        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());
    }

    @Test
    void shouldFailWhenAllExecutorsSucceededAndMessagesFailed() {
        // arrange
        when(databaseExecutors1.execute(any(), any())).thenAnswer(invocationOnMock -> {
            Consumer<QueryResultItem> argument = invocationOnMock.getArgument(1, Consumer.class);
            argument.accept(new QueryResultItem(null, 1, true));
            return Future.succeededFuture();
        });
        when(databaseExecutors2.execute(any(), any())).thenAnswer(invocationOnMock -> {
            Consumer<QueryResultItem> argument = invocationOnMock.getArgument(1, Consumer.class);
            argument.accept(new QueryResultItem(null, 1, true));
            return Future.succeededFuture();
        });
        when(upstream.push(Mockito.any(), Mockito.any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));


        // act
        Future<Void> result = queryExecutorService.execute(new QueryRequest());

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("Unexpected success"));
        }

        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

    @Test
    void shouldFailWhenOneExecutorFailedAndMessagesSucceeded() {
        // arrange
        when(databaseExecutors1.execute(any(), any())).thenAnswer(invocationOnMock -> Future.failedFuture(new RuntimeException("Exception")));
        when(databaseExecutors2.execute(any(), any())).thenAnswer(invocationOnMock -> {
            Consumer<QueryResultItem> argument = invocationOnMock.getArgument(1, Consumer.class);
            argument.accept(new QueryResultItem(null, 1, true));
            return Future.succeededFuture();
        });
        when(upstream.push(Mockito.any(), Mockito.any())).thenReturn(Future.succeededFuture());


        // act
        Future<Void> result = queryExecutorService.execute(new QueryRequest());

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("Unexpected success"));
        }

        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

}