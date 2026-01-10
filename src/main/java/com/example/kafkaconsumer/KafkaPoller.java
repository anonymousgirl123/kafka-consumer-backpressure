package com.example.kafkaconsumer;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

@Component
public class KafkaPoller {

    private final Properties props;
    private final RecordQueue queue;
    private final OffsetTracker tracker;

    public KafkaPoller(
            @Qualifier("kafkaConsumerProps") Properties props,
            RecordQueue queue,
            OffsetTracker tracker) {
        this.props = props;
        this.queue = queue;
        this.tracker = tracker;
    }

    @PostConstruct
    public void start() {
        new Thread(this::pollLoop, "kafka-poll-thread").start();
    }

    private void pollLoop() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("orders"));

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    queue.enqueue(record);
                }

                // 🔑 Commit ONLY from the polling consumer
                Map<TopicPartition, OffsetAndMetadata> offsets =
                        tracker.snapshot();

                if (!offsets.isEmpty()) {
                    consumer.commitAsync(offsets, null);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
