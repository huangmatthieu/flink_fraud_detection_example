# Flink Experiment
---

## Tech Stack

- Apache Flink 1.17.x
- Scala 2.12
- Maven
- (Optional) Apache Kafka

---

## Project Structure

```
src/
├── main/
│   ├── scala/
│   │   └── io/github/flinkexample/frauddetection/ 
│   │       ├── model
│   │       │   ├── Alert.scala
│   │       │   └── Transaction.scala
│   │       ├── sink
│   │       │   └── KafkaSinkUtils.scala
│   │       ├── source
│   │       │   └── KafkaSourceUtils.scala
│   │       ├── transformations
│   │       │   └── FrandDetection.scala
│   │       └── FraudDetectionMain.scala
├── pom.xml
└── README.md  
```

### Flink Fraud Detection (Scala)

A simple real-time fraud detection pipeline built with Apache Flink.

This project demonstrates how to:
- Process streaming data with Flink
- Use keyed state (`ValueState`)
- Apply event-time processing with watermarks
- Apply state TTL to avoid unbounded state growth
- Detect fraud patterns in real time
- (Optional) Send alerts to Kafka

---

## Use Case

We detect suspicious activity when:

> A user performs **2 transactions > 1000€ within 1 minute**

>A user performs **transactions that occur too far in a short time window**

This is implemented using **stateful stream processing**, not batch.

---

##  Architecture

```
Transaction Stream -> keyBy(userId) -> Stateful Fraud Detection -> Alerts -> (Console/Kafka)
```

## Kafka Setup
We can use the following commands to create the new topics called transactions and fraud-alerts:
```
# Create transaction topic
kafka-topics.sh --create \
    --topic transactions \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1

# Create alerts topic
kafka-topics.sh --create \
    --topic fraud-alerts \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
```
