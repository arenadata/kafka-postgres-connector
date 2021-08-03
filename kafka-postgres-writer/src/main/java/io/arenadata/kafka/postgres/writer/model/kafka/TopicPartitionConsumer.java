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
package io.arenadata.kafka.postgres.writer.model.kafka;

import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

@Data
@Slf4j
public class TopicPartitionConsumer {
    private final Set<Long> notCommittedOffsets = new ConcurrentSkipListSet<>();
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final TopicPartition topicPartition;
    private long lastOffset;

    public TopicPartitionConsumer(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                  TopicPartition topicPartition,
                                  long lastOffset) {
        this.kafkaConsumer = kafkaConsumer;
        this.topicPartition = topicPartition;
        this.lastOffset = lastOffset;
    }

    public Optional<Map<TopicPartition, OffsetAndMetadata>> getCompletedOffset() {
        log.debug("TopicPartition[{}]: lastOffset[{}]", topicPartition, lastOffset);
        val iterator = notCommittedOffsets.iterator();
        var commitOffset = lastOffset;
        while (iterator.hasNext()) {
            Long offset = iterator.next();
            if (offset == commitOffset) {
                commitOffset++;
                iterator.remove();
            } else {
                break;
            }
        }
        if (lastOffset == commitOffset) {
            log.debug("TopicPartition[{}]: lastOffset[{}] - notCommittedOffsets[{}]", topicPartition, lastOffset, notCommittedOffsets);
            return Optional.empty();
        } else {
            lastOffset = commitOffset;
            log.debug("TopicPartition[{}]: commitOffset[{}]", topicPartition, lastOffset);
            val map = new HashMap<TopicPartition, OffsetAndMetadata>();
            map.put(topicPartition, new OffsetAndMetadata(lastOffset, null));
            return Optional.of(map);
        }
    }

    public void addCompletedOffset(long offset) {
        notCommittedOffsets.add(offset);
    }
}
