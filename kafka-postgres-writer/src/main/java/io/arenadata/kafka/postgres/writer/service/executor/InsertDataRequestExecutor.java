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
package io.arenadata.kafka.postgres.writer.service.executor;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.arenadata.kafka.postgres.writer.configuration.properties.VerticleProperties;
import io.arenadata.kafka.postgres.writer.factory.InsertRequestFactory;
import io.arenadata.kafka.postgres.writer.model.DataTopic;
import io.arenadata.kafka.postgres.writer.model.InsertDataContext;
import io.arenadata.kafka.postgres.writer.model.kafka.InsertChunk;
import io.arenadata.kafka.postgres.writer.model.kafka.TopicPartitionConsumer;
import io.arenadata.kafka.postgres.writer.repository.InsertDataContextRepository;
import io.arenadata.kafka.postgres.writer.service.kafka.KafkaConsumerService;
import io.arenadata.kafka.postgres.writer.verticle.ConfigurableVerticle;
import io.arenadata.kafka.postgres.writer.verticle.InsertVerticle;
import io.arenadata.kafka.postgres.writer.verticle.KafkaCommitVerticle;
import io.arenadata.kafka.postgres.writer.verticle.KafkaConsumerVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.common.TopicPartition;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

@Slf4j
@Service
@AllArgsConstructor
public class InsertDataRequestExecutor {
    private final InsertDataContextRepository dataContextRepository;
    private final InsertRequestFactory insertRequestFactory;
    private final VerticleProperties verticleProperties;
    private final KafkaConsumerService consumerService;
    private final List<PostgresExecutor> postgresExecutors;
    private final Vertx vertxCore;

    public void execute(InsertDataContext context) {
        dataContextRepository.add(context);
        List<String> columnsList = context.getRequest().getSchema().getFields()
                .stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());
        log.debug("Received columnList: [{}]", columnsList);
        context.setColumnsList(columnsList);
        runKafkaProcess(context);
    }

    private void runKafkaProcess(InsertDataContext context) {
        consumerService.getTopicPartitions(context)
                .onSuccess(topicPartitions -> {

                    Queue<InsertChunk> insertChunkQueue = new ConcurrentLinkedDeque<>();
                    HashMap<TopicPartition, TopicPartitionConsumer> consumerMap = new HashMap<>();

                    List<ConfigurableVerticle> insertDataVerticles = topicPartitions.stream()
                            .map(topicPartition -> KafkaConsumerVerticle.builder()
                                    .workerProperties(verticleProperties.getConsumer())
                                    .insertRequestFactory(insertRequestFactory)
                                    .insertChunkQueue(insertChunkQueue)
                                    .consumerService(consumerService)
                                    .partitionInfo(topicPartition)
                                    .consumerMap(consumerMap)
                                    .context(context)
                                    .build())
                            .collect(Collectors.toList());

                    insertDataVerticles.add(
                            KafkaCommitVerticle.builder()
                                    .workerProperties(verticleProperties.getCommit())
                                    .insertRequestFactory(insertRequestFactory)
                                    .consumerService(consumerService)
                                    .consumerMap(consumerMap)
                                    .context(context)
                                    .build()
                    );

                    postgresExecutors.stream()
                            .map(executor -> InsertVerticle.builder()
                                    .executor(executor)
                                    .properties(verticleProperties.getInsert())
                                    .insertRequestFactory(insertRequestFactory)
                                    .insertChunkQueue(insertChunkQueue)
                                    .context(context)
                                    .build())
                            .forEach(insertDataVerticles::add);

                    CompositeFuture.join(
                            insertDataVerticles.stream()
                                    .map(this::getVerticleFuture)
                                    .collect(Collectors.toList())
                    ).onSuccess(success -> {
                        context.getVerticleIds().addAll(success.list());
                        vertxCore.eventBus().publish(KafkaConsumerVerticle.START_TOPIC + context.getContextId(), "");
                        vertxCore.eventBus().publish(InsertVerticle.INSERT_START_TOPIC + context.getContextId(), "");
                        vertxCore.eventBus().publish(KafkaCommitVerticle.START_COMMIT + context.getContextId(), "");
                        sendCompletedResponse(context);
                    }).onFailure(error -> error(context, error));
                })
                .onFailure(e -> {
                    log.error("Topic Subscription Error [{}]: {}", context.getRequest().getKafkaTopic(), e.getMessage(), e);
                    error(context, e);
                });
    }

    private Future<String> getVerticleFuture(ConfigurableVerticle kafkaConsumerVerticle) {
        return Future.future((Promise<String> p) ->
                vertxCore.deployVerticle(kafkaConsumerVerticle,
                        kafkaConsumerVerticle.getDeploymentOptions(),
                        p));
    }

    private void sendCompletedResponse(InsertDataContext context) {
        try {
            context.getContext().response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(200)
                    .end(DatabindCodec.mapper().writeValueAsString(context.toNewDataResult()));
        } catch (JsonProcessingException e) {
            log.error("Can't send completed response [{}]: {}", context.getRequest().getKafkaTopic(), e.getMessage(), e);
            error(context, e);
        }
    }

    private void error(InsertDataContext context, Throwable throwable) {
        val contextId = context.getContextId();
        context.setCause(throwable);
        log.error("Error consuming message in context: {}", context, throwable);
        stop(contextId);
    }

    private void stop(String contextId) {
        vertxCore.eventBus().publish(DataTopic.SEND_RESPONSE.getValue(), contextId);
    }
}
