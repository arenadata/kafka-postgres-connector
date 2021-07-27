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
package io.arenadata.kafka.postgres.writer.configuration;

import io.arenadata.kafka.postgres.writer.configuration.properties.kafka.KafkaProperties;
import io.arenadata.kafka.postgres.writer.factory.VertxKafkaConsumerFactory;
import io.vertx.core.Vertx;
import lombok.var;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Configuration
public class KafkaConfiguration {
    @Bean
    public VertxKafkaConsumerFactory<byte[], byte[]> byteArrayConsumerFactory(KafkaProperties kafkaProperties, Vertx vertx) {
        var props = new HashMap<>(kafkaProperties.getConsumer().getProperty());
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new VertxKafkaConsumerFactory<>(vertx, props);
    }
}
