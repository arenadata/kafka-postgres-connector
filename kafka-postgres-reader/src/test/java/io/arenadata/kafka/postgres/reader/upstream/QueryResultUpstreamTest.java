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
package io.arenadata.kafka.postgres.reader.upstream;

import io.arenadata.kafka.postgres.avro.codec.AvroQueryResultEncoder;
import io.arenadata.kafka.postgres.avro.model.DtmQueryResponseMetadata;
import io.arenadata.kafka.postgres.reader.model.QueryRequest;
import io.arenadata.kafka.postgres.reader.model.QueryResultItem;
import io.arenadata.kafka.postgres.reader.service.PublishService;
import io.vertx.core.Future;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryResultUpstreamTest {
    private static final String TOPIC = "topic";
    private static final int CHUNK_NUMBER = 0;
    private static final boolean IS_LAST_CHUNK = true;
    private static final int STREAM_NUMBER = 1;
    private static final int STREAM_TOTAL = 2;
    private static final String TABLE_NAME = "table";

    @Mock
    private PublishService publishService;
    @Mock
    private Schema schema;
    @Mock
    private AvroQueryResultEncoder resultEncoder;
    @InjectMocks
    private QueryResultUpstream queryResultUpstream;

    @Captor
    private ArgumentCaptor<DtmQueryResponseMetadata> dtmQueryResponseMetadataArgumentCaptor;

    @Test
    void shouldSuccessWhenPublishSuccess() {
        // arrange
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setKafkaTopic(TOPIC);
        queryRequest.setStreamNumber(STREAM_NUMBER);
        queryRequest.setStreamTotal(STREAM_TOTAL);
        queryRequest.setTable(TABLE_NAME);
        queryRequest.setKafkaBrokers(Collections.emptyList());

        QueryResultItem resultItem = new QueryResultItem(Collections.emptyList(), CHUNK_NUMBER, IS_LAST_CHUNK);

        when(resultEncoder.encode(anyList(), same(schema))).thenReturn(new byte[0]);
        when(publishService.publishQueryResult(anyList(), eq(TOPIC), dtmQueryResponseMetadataArgumentCaptor.capture(), any()))
                .thenReturn(Future.succeededFuture());

        // act
        Future<Void> result = queryResultUpstream.push(queryRequest, resultItem);

        // assert
        assertTrue(result.isComplete());
        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());

        DtmQueryResponseMetadata dtmQueryResponseMetadata = dtmQueryResponseMetadataArgumentCaptor.getValue();
        assertNotNull(dtmQueryResponseMetadata);
        assertEquals(CHUNK_NUMBER, dtmQueryResponseMetadata.getChunkNumber());
        assertEquals(IS_LAST_CHUNK, dtmQueryResponseMetadata.getIsLastChunk());
        assertEquals(STREAM_NUMBER, dtmQueryResponseMetadata.getStreamNumber());
        assertEquals(STREAM_TOTAL, dtmQueryResponseMetadata.getStreamTotal());
        assertSame(TABLE_NAME, dtmQueryResponseMetadata.getTableName());
        assertNotNull(dtmQueryResponseMetadata.getSchema());
    }

    @Test
    void shouldFailWithProperMessageWhenEncoderFailed() {
        // arrange
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setKafkaTopic(TOPIC);
        queryRequest.setStreamNumber(STREAM_NUMBER);
        queryRequest.setStreamTotal(STREAM_TOTAL);
        queryRequest.setTable(TABLE_NAME);
        queryRequest.setKafkaBrokers(Collections.emptyList());
        queryRequest.setKafkaBrokers(Collections.emptyList());

        QueryResultItem resultItem = new QueryResultItem(Collections.emptyList(), 0, true);

        when(resultEncoder.encode(anyList(), same(schema))).thenThrow(new RuntimeException("Exception"));

        // act
        Future<Void> result = queryResultUpstream.push(queryRequest, resultItem);

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("Unexpected success"));
        }

        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

    @Test
    void shouldSuccessWhenPublishFailed() {
        // arrange
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setKafkaTopic(TOPIC);
        queryRequest.setStreamNumber(STREAM_NUMBER);
        queryRequest.setStreamTotal(STREAM_TOTAL);
        queryRequest.setTable(TABLE_NAME);
        queryRequest.setKafkaBrokers(Collections.emptyList());
        queryRequest.setKafkaBrokers(Collections.emptyList());

        QueryResultItem resultItem = new QueryResultItem(Collections.emptyList(), 0, true);

        when(resultEncoder.encode(anyList(), same(schema))).thenReturn(new byte[0]);
        when(publishService.publishQueryResult(anyList(), eq(TOPIC), any(), any()))
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        Future<Void> result = queryResultUpstream.push(queryRequest, resultItem);

        // assert
        assertTrue(result.isComplete());
        if (result.succeeded()) {
            fail(new AssertionError("Unexpected success"));
        }

        assertTrue(result.failed());
        assertSame(RuntimeException.class, result.cause().getClass());
    }

}