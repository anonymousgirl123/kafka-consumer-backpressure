package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Component
public class RecordQueue {

    private final BlockingQueue<ConsumerRecord<String, String>> queue =
            new ArrayBlockingQueue<>(2000);

    public void enqueue(ConsumerRecord<String, String> record) throws InterruptedException {
        queue.put(record);
    }

    public ConsumerRecord<String, String> dequeue() throws InterruptedException {
        return queue.take();
    }

    public int depth() {
        return queue.size();
    }
}