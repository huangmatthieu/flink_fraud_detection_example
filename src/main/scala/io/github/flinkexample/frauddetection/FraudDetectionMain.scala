package io.github.flinkexample.frauddetection

import io.github.flinkexample.frauddetection.model.Transaction
import io.github.flinkexample.frauddetection.transformations.FraudDetector
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala._

import java.time.Duration


object FraudDetectionMain {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Enable event time
    env.setParallelism(1)

    val transactions: DataStream[Transaction] =
      env.fromElements(
        Transaction("user1", 1200, 1000L),
        Transaction("user1", 1300, 50000L), // within 1 min → fraud
        Transaction("user2", 200, 2000L),
        Transaction("user1", 1500, 200000L) // outside window
      )
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
            .forBoundedOutOfOrderness[Transaction](Duration.ofSeconds(10))
            .withTimestampAssigner(new SerializableTimestampAssigner[Transaction] {
              override def extractTimestamp(t: Transaction, l: Long): Long = t.timestamp
            } )
        )

    val alerts = transactions
      .keyBy(_.userId)
      .process(new FraudDetector)

    alerts
      .map(_.toJson)
      //.sinkTo(Kafka.sink("localhost:9292", "fraud-alerts"))
      .print()

    env.execute("Flink Fraud Detection")

  }
}
