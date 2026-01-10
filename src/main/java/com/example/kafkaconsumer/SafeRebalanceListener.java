package com.example.kafkaconsumer;


import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

public class SafeRebalanceListener implements ConsumerRebalanceListener {

    private final KafkaConsumer<String, String> consumer;
    private final RecordQueue queue;
    private final OffsetTracker tracker;

    public SafeRebalanceListener(
            KafkaConsumer<String, String> consumer,
            RecordQueue queue,
            OffsetTracker tracker) {
        this.consumer = consumer;
        this.queue = queue;
        this.tracker = tracker;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("⚠ Partitions revoked: " + partitions);

        // 1️⃣ Stop intake
        consumer.pause(partitions);

        // 2️⃣ Drain in-flight work
        while (queue.depth() > 0) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
            }
        }

        // 3️⃣ Commit offsets for revoked partitions
        Map<TopicPartition, OffsetAndMetadata> offsets =
                tracker.snapshotFor(partitions);

        if (!offsets.isEmpty()) {
            consumer.commitSync(offsets);
            System.out.println("✅ Committed offsets on revoke: " + offsets);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("▶ Partitions assigned: " + partitions);

        // Resume newly assigned partitions
        consumer.resume(partitions);
    }
}
