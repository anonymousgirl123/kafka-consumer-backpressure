package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class WorkerPool {

    private final ExecutorService workers = Executors.newFixedThreadPool(8);
    private final RecordQueue queue;
    private final OffsetTracker tracker;

    public WorkerPool(RecordQueue queue, OffsetTracker tracker) {
        this.queue = queue;
        this.tracker = tracker;
    }

    @PostConstruct
    public void start() {
        for (int i = 0; i < 8; i++) {
            workers.submit(this::workerLoop);
        }
    }

    private void workerLoop() {
        while (true) {
            try {
                ConsumerRecord<String, String> record = queue.dequeue();
                Thread.sleep(100);
                tracker.markProcessed(record);
            } catch (Exception ignored) {}
        }
    }
}