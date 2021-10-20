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
import io.arenadata.kafka.postgres.avro.model.AvroQueryResultRow;
import io.arenadata.kafka.postgres.avro.model.DtmQueryResponseMetadata;
import io.arenadata.kafka.postgres.reader.model.QueryRequest;
import io.arenadata.kafka.postgres.reader.model.QueryResultItem;
import io.arenadata.kafka.postgres.reader.service.PublishService;
import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;

import java.util.stream.Collectors;

@Slf4j
public class QueryResultUpstream implements Upstream<QueryResultItem> {

    private final PublishService publishService;
    private final Schema schema;
    private final AvroQueryResultEncoder resultEncoder;
    private final KafkaProducer<byte[], byte[]> kafkaProducer;

    public QueryResultUpstream(PublishService publishService,
                               Schema schema,
                               AvroQueryResultEncoder resultEncoder,
                               KafkaProducer<byte[], byte[]> kafkaProducer) {
        this.publishService = publishService;
        this.schema = schema;
        this.resultEncoder = resultEncoder;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public Future<Void> push(QueryRequest queryRequest, QueryResultItem item) {
        return Future.future(promise -> {
            val bytes = resultEncoder.encode(item.getDataSet().stream()
                    .map(row -> new AvroQueryResultRow(schema, row))
                    .collect(Collectors.toList()), schema);
            val response = new DtmQueryResponseMetadata(queryRequest.getTable(),
                    queryRequest.getStreamNumber(),
                    queryRequest.getStreamTotal(),
                    item.getChunkNumber(),
                    item.getIsLastChunk());
            publishService.publishQueryResult(kafkaProducer, queryRequest.getKafkaTopic(), response, bytes)
                    .onComplete(promise);
        });
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}
