package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OffsetTracker {

    private final Map<TopicPartition, Long> processedOffsets = new ConcurrentHashMap<>();

    public void markProcessed(ConsumerRecord<?, ?> record) {
        TopicPartition tp =
                new TopicPartition(record.topic(), record.partition());

        processedOffsets.merge(tp, record.offset(), Math::max);
    }

    public Map<TopicPartition, OffsetAndMetadata> snapshot() {
        Map<TopicPartition, OffsetAndMetadata> snapshot = new HashMap<>();
        processedOffsets.forEach(
                (tp, offset) -> snapshot.put(tp, new OffsetAndMetadata(offset + 1))
        );
        return snapshot;
    }

    public Map<TopicPartition, OffsetAndMetadata> snapshotFor(
            Collection<TopicPartition> partitions) {

        Map<TopicPartition, OffsetAndMetadata> snapshot = new HashMap<>();
        for (TopicPartition tp : partitions) {
            Long offset = processedOffsets.get(tp);
            if (offset != null) {
                snapshot.put(tp, new OffsetAndMetadata(offset + 1));
            }
        }
        return snapshot;
    }
}
