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
package io.arenadata.kafka.postgres.writer.service.kafka;

import io.arenadata.kafka.postgres.writer.configuration.properties.kafka.KafkaProperties;
import io.arenadata.kafka.postgres.writer.factory.VertxKafkaConsumerFactory;
import io.arenadata.kafka.postgres.writer.model.InsertDataContext;
import io.arenadata.kafka.postgres.writer.model.kafka.KafkaBrokerInfo;
import io.arenadata.kafka.postgres.writer.model.kafka.TopicPartitionConsumer;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service
@AllArgsConstructor
public class KafkaConsumerService {
    private static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    private final VertxKafkaConsumerFactory<byte[], byte[]> consumerFactory;
    private final KafkaProperties kafkaProperties;

    public Future<List<PartitionInfo>> getTopicPartitions(InsertDataContext context) {
        return Future.future(p -> {
            val consumerProperty = getConsumerProperty(context);
            val topicName = context.getRequest().getKafkaTopic();
            val partitionInfoConsumer = createConsumer(consumerProperty, topicName);
            partitionInfoConsumer.partitionsFor(topicName)
                    .onComplete(ar -> {
                        partitionInfoConsumer.close();
                        if (ar.succeeded()) {
                            p.complete(ar.result());
                        } else {
                            val errMsg = String.format("Can't get partition info by topic [%s]", topicName);
                            log.error(errMsg, ar.cause());
                            p.fail(ar.cause());
                        }
                    });
        });
    }

    private KafkaConsumer<byte[], byte[]> createConsumer(Map<String, String> consumerProperty,
                                                         String topicName) {
        val consumer = consumerFactory.create(consumerProperty);
        consumer.subscribe(topicName, it -> {
            if (it.succeeded()) {
                log.debug("Successful topic subscription {}", topicName);
            } else {
                log.error("Topic Subscription Error {}: {}", topicName, it.cause());
            }
        });
        consumer.exceptionHandler(it -> log.error("Error reading message from topic {}", topicName, it));
        return consumer;
    }

    public Future<TopicPartitionConsumer> createTopicPartitionConsumer(InsertDataContext context, PartitionInfo partitionInfo) {
        return createTopicPartitionConsumer(getConsumerProperty(context), partitionInfo);
    }

    private Future<TopicPartitionConsumer> createTopicPartitionConsumer(Map<String, String> consumerProperty,
                                                                        PartitionInfo partitionInfo) {
        val consumer = consumerFactory.create(consumerProperty);
        consumer.handler(event -> log.error("UNEXPECTED MEESSAGE READE"));
        consumer.exceptionHandler(it -> log.error("Error reading message from TopicPartition {}", partitionInfo, it));
        val topicPartition = new TopicPartition(partitionInfo.getTopic(), partitionInfo.getPartition());
        return assign(consumer, topicPartition)
                .compose(v -> getPosition(consumer, topicPartition))
                .map(beginningOffset -> new TopicPartitionConsumer(consumer, topicPartition, beginningOffset))
                .onSuccess(topicPartitionConsumer -> log.info("created consumer[{}] by topicPartition [{}]", topicPartitionConsumer, topicPartition));
    }

    private Future<Void> assign(KafkaConsumer<byte[], byte[]> consumer, TopicPartition topicPartition) {
        return Future.future((Promise<Void> p) -> consumer.assign(topicPartition, p));
    }

    private Future<Long> getPosition(KafkaConsumer<byte[], byte[]> consumer, TopicPartition topicPartition) {
        return Future.future((Promise<Long> p) -> consumer.position(topicPartition, p));
    }

    private HashMap<String, String> getConsumerProperty(InsertDataContext context) {
        val props = new HashMap<>(kafkaProperties.getConsumer().getProperty());
        updateGroupId(context, props);
        List<String> brokerAddresses = context.getRequest().getKafkaBrokers().stream()
                .map(KafkaBrokerInfo::getAddress).collect(toList());
        props.put(BOOTSTRAP_SERVERS_KEY, String.join(",", brokerAddresses));
        return props;
    }

    private void updateGroupId(InsertDataContext context, Map<String, String> props) {
        props.put("group.id", context.getRequest().getConsumerGroup());
    }
}
