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
package io.arenadata.kafka.postgres.writer.verticle;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.kafka.postgres.writer.configuration.properties.VerticleProperties;
import io.arenadata.kafka.postgres.writer.factory.InsertRequestFactory;
import io.arenadata.kafka.postgres.writer.model.DataTopic;
import io.arenadata.kafka.postgres.writer.model.InsertDataContext;
import io.arenadata.kafka.postgres.writer.model.kafka.PartitionOffset;
import io.arenadata.kafka.postgres.writer.model.kafka.TopicPartitionConsumer;
import io.arenadata.kafka.postgres.writer.service.kafka.KafkaConsumerService;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.common.TopicPartition;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;


@Slf4j
@Getter
@Builder
public class KafkaCommitVerticle extends ConfigurableVerticle {
    public static final String START_COMMIT = "kafka_commit_start";
    public static final String KAFKA_COMMIT_TOPIC = "kafka_consumer_offset_commit";
    private final VerticleProperties.CommitWorkerProperties workerProperties;
    private final HashMap<TopicPartition, TopicPartitionConsumer> consumerMap;
    private final InsertRequestFactory insertRequestFactory;
    private final String id = UUID.randomUUID().toString();
    private final Queue<PartitionOffset> offsetsQueue = new ConcurrentLinkedDeque<>();
    private final KafkaConsumerService consumerService;
    private final InsertDataContext context;
    private volatile long timerId;
    private volatile boolean stopped;

    @Override
    public void start() {
        vertx.eventBus().consumer(START_COMMIT + context.getContextId(), ar -> init());
        vertx.eventBus().consumer(KAFKA_COMMIT_TOPIC + context.getContextId(), this::collectKafkaMessages);
    }

    private void collectKafkaMessages(Message<Object> partitionOffsets) {
        if (stopped) {
            return;
        }

        try {
            List<PartitionOffset> offsets = DatabindCodec.mapper()
                    .readValue(partitionOffsets.body().toString(), new TypeReference<List<PartitionOffset>>() {
                    });
            offsetsQueue.addAll(offsets);
        } catch (JsonProcessingException e) {
            log.error("Deserialize PartitionOffsets error [{}]: {}", partitionOffsets, e);
            error(context, e);
        }
    }

    private void init() {
        if (stopped) {
            return;
        }

        timerId = vertx.setPeriodic(1000, timer -> {
            List<PartitionOffset> byCommit = new ArrayList<>();
            while (!offsetsQueue.isEmpty()) {
                PartitionOffset offset = offsetsQueue.poll();
                if (offset != null) {
                    byCommit.add(offset);
                } else {
                    break;
                }
            }
            List<PartitionOffset> notExistsConsumers = commitKafkaMessages(byCommit);
            offsetsQueue.addAll(notExistsConsumers);
        });
    }

    @Override
    public void stop() {
        stopped = true;
        vertx.cancelTimer(timerId);
    }

    private List<PartitionOffset> commitKafkaMessages(List<PartitionOffset> partitionOffsets) {
        Map<TopicPartition, List<PartitionOffset>> partitionListMap = partitionOffsets.stream()
                .collect(Collectors.groupingBy(PartitionOffset::getPartition, Collectors.toList()));

        List<PartitionOffset> notExistsConsumers = partitionListMap.entrySet().stream()
                .filter(e -> !consumerMap.containsKey(e.getKey()))
                .flatMap(e -> e.getValue().stream())
                .collect(Collectors.toList());

        consumerMap.values().forEach(topicPartitionConsumer -> {
            if (partitionListMap.containsKey(topicPartitionConsumer.getTopicPartition())) {

                partitionListMap.get(topicPartitionConsumer.getTopicPartition())
                        .forEach(partitionOffset -> topicPartitionConsumer.addCompletedOffset(partitionOffset.getOffset()));

                String topic = topicPartitionConsumer.getTopicPartition().getTopic();
                topicPartitionConsumer.getCompletedOffset().ifPresent(offsetAndMetadataMap -> {
                    topicPartitionConsumer.getKafkaConsumer().commit(offsetAndMetadataMap, ar -> {
                        if (ar.succeeded()) {
                            log.debug("Message commit for topic [{}] was successful. Offsets[{}]", topic, offsetAndMetadataMap);
                        } else {
                            log.debug("Message commit error for topic [{}]: [{}]", topic, ar.cause());
                        }
                    });
                });
            }
            topicPartitionConsumer.getKafkaConsumer().resume();
        });
        return notExistsConsumers;
    }

    private void error(InsertDataContext context, Throwable throwable) {
        val contextId = context.getContextId();
        context.setCause(throwable);
        log.error("Error consuming message in context: {}", context, throwable);
        stop(contextId);
    }

    private void stop(String contextId) {
        vertx.eventBus().publish(DataTopic.SEND_RESPONSE.getValue(), contextId);
    }

    @Override
    public DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("kafka-commit-worker-pool")
                .setWorkerPoolSize(workerProperties.getPoolSize());
    }
}
