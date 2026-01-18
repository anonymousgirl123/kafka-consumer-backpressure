# kafka-consumer-backpressure 
### A backpressure-aware, rebalance-safe Kafka consumer that decouples polling from processing and commits offsets only after downstream completion.

# Kafka Consumer with Backpressure & Rebalance Safety

A **production-grade Kafka consumer architecture** designed to eliminate consumer lag, rebalance storms, and offset inconsistencies under high-throughput workloads.

This project demonstrates **correct Kafka protocol usage**, **explicit backpressure**, and **safe concurrency design**, going far beyond naïve consumer implementations.

<img width="1304" height="511" alt="image" src="https://github.com/user-attachments/assets/2b903716-5e46-49ee-bed3-e7c5bc814bba" />

---

## 🚩 Problem Statement

In high-throughput Kafka systems, consumer lag often grows even when brokers are healthy and multiple consumers are running. Common root causes include:

- Poll loop blocked by slow downstream processing
- Unbounded in-memory queues
- Unsafe offset commits
- Frequent rebalance storms
- In-flight records lost during partition reassignments

Scaling consumers or adding threads typically **does not solve the problem** and often makes it worse.

---

## 🎯 Design Goals

- Keep the Kafka poll loop **non-blocking**
- Apply **explicit backpressure**
- Commit offsets **only after successful processing**
- Prevent rebalance storms
- Handle rebalances **safely and deterministically**
- Preserve **at-least-once delivery guarantees**

---

## 🧠 Architectural Evolution

### ❌ Before (Naïve Consumer)
- Polling, processing, and committing in the same thread
- Slow processing blocks `poll()`
- Consumer group instability
- Lag accumulates on hot partitions
- Scaling yields diminishing returns

### ✅ After (Final Architecture)
- Polling decoupled from processing
- Bounded queue with backpressure
- Parallel worker pool
- Manual offset tracking and commit
- Partition pause / resume
- Rebalance-safe draining

This redesign restores **predictable scaling and stability**.

---

## 🏗 Final Architecture (High Level)

 <img width="403" height="533" alt="image" src="https://github.com/user-attachments/assets/4e793d62-6bb2-4ba0-99bd-963077a72173" />


## 🏗 Sequence Diagram

 <img width="529" height="266" alt="image" src="https://github.com/user-attachments/assets/7af7074b-bd61-43b6-ba5a-9cc74d4f2f5b" />




### Key Invariants
- Only **one KafkaConsumer instance**
- Only the **poll thread interacts with Kafka APIs**
- Worker threads are **Kafka-agnostic**
- Offsets are committed **after processing**, not on poll

---

## 🧩 Core Components

### Kafka Poll Thread
- Polls records
- Applies pause / resume
- Commits offsets
- Handles rebalance callbacks

### Bounded Record Queue
- Fixed capacity
- Enforces backpressure
- Protects poll loop from downstream slowness

### Worker Pool
- Parallel processing
- CPU / IO heavy work
- No Kafka access

### Offset Tracker
- Tracks highest processed offset per partition
- Supports rebalance-safe commits

### Rebalance Listener
- Pauses intake on revoke
- Drains in-flight records
- Commits offsets safely
- Resumes on assignment

---

## ⏸ Backpressure with Pause / Resume

When downstream pressure increases:
1. Queue depth exceeds threshold
2. Poll thread pauses assigned partitions
3. Workers drain in-flight records
4. Queue depth drops
5. Poll thread resumes partitions

This prevents poll starvation and rebalance storms.

---

## 🔄 Rebalance Safety

During rebalances:
- Intake is paused
- In-flight work is drained
- Offsets for revoked partitions are committed
- New partitions resume cleanly

This ensures:
- No offset loss
- No commit failures
- Stable group membership

---

## 🔐 Processing Guarantees

- **At-least-once delivery**
- No message loss
- Controlled duplicates (downstream idempotency expected)
- Stable consumer group behavior

---

## 🧪 Tested Scenarios

- Slow processing
- Burst traffic
- Queue saturation
- Consumer restart mid-processing
- Rebalance during load

In all scenarios:
- Lag stabilized
- No rebalance storms
- Correct offset progression

---

## 🚀 How to Run Locally

### Start Kafka (Docker)
```bash
docker compose up -d

Run the Consumer
mvn clean spring-boot:run

Produce Messages
docker exec -it kafka kafka-console-producer \
  --topic orders \
  --bootstrap-server localhost:9092




