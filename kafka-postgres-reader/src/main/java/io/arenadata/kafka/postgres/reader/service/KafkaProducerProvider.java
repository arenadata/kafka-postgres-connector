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

import io.arenadata.kafka.postgres.reader.configuration.properties.KafkaProperties;
import io.arenadata.kafka.postgres.reader.factory.VertxKafkaProducerFactory;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.val;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class KafkaProducerProvider {

    private final Map<String, VertxKafkaProducerFactory<byte[], byte[]>> kafkaProducerMap = new ConcurrentHashMap<>();
    private final KafkaProperties kafkaProperties;
    private final Vertx vertx;

    @Autowired
    public KafkaProducerProvider(KafkaProperties kafkaProperties, Vertx vertx) {
        this.vertx = vertx;
        this.kafkaProperties = kafkaProperties;
    }

    public KafkaProducer<byte[], byte[]> create(String brokersList) {
        val kafkaProducer = kafkaProducerMap.computeIfAbsent(brokersList, this::getKafkaProducerFactory);
        return kafkaProducer.create(kafkaProperties.getProperty());
    }

    private VertxKafkaProducerFactory<byte[], byte[]> getKafkaProducerFactory(String brokersList) {
        var props = new HashMap<>(kafkaProperties.getProperty());
        props.put("bootstrap.servers", brokersList);
        return new VertxKafkaProducerFactory<>(this.vertx, props);
    }
}
