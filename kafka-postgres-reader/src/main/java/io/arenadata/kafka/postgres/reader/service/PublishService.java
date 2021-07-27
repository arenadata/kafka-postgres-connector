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

import io.arenadata.kafka.postgres.avro.codec.AvroEncoder;
import io.arenadata.kafka.postgres.avro.model.DtmQueryResponseMetadata;
import io.arenadata.kafka.postgres.reader.model.KafkaBrokerInfo;
import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class PublishService {
    private final KafkaProducerProvider kafkaProducerProvider;
    private final AvroEncoder<DtmQueryResponseMetadata> writer;

    @Autowired
    public PublishService(KafkaProducerProvider kafkaProducerProvider) {
        this.kafkaProducerProvider = kafkaProducerProvider;
        this.writer = new AvroEncoder<>();
    }

    public Future<Void> publishQueryResult(List<KafkaBrokerInfo> kafkaBrokers,
                                           String topicName,
                                           DtmQueryResponseMetadata key,
                                           byte[] message) {
        return Future.future(promise -> {
            log.trace("Send chunk [{}] for table [{}] into topic [{}]", key.getChunkNumber(), key.getTableName(), topicName);
            val kafkaBrokersListStr = kafkaBrokers.stream().map(KafkaBrokerInfo::getAddress)
                    .collect(Collectors.joining(","));
            val encodedKey = writer.encode(Collections.singletonList(key), key.getSchema());
            val record = KafkaProducerRecord.create(topicName, encodedKey, message);
            val kafkaProducer = kafkaProducerProvider.getOrCreate(kafkaBrokersListStr);
            kafkaProducer.send(record)
                    .onSuccess(event -> promise.complete())
                    .onFailure(promise::fail);
        });
    }

}
