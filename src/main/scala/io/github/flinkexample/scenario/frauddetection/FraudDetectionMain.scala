package io.github.flinkexample.scenario.frauddetection

import io.github.flinkexample.io.sink.KafkaSinkUtils
import io.github.flinkexample.io.source.KafkaSourceUtils
import io.github.flinkexample.scenario.frauddetection.model.Transaction
import io.github.flinkexample.scenario.frauddetection.transformations.FraudDetector
import io.github.flinkexample.utils.JsonParser
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala._

import java.time.Duration
import org.slf4j.{Logger, LoggerFactory}


object FraudDetectionMain {

  val logger: Logger = LoggerFactory.getLogger("fraudDetection")
  val KAFKA_BOOTSTRAP_SERVER_EXTERNAL="localhost:9092"


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Enable event time
    env.setParallelism(1)

    val streamTx: DataStream[Transaction] =
      env
        /*.fromSource(KafkaSourceUtils.source(KAFKA_BOOTSTRAP_SERVER_EXTERNAL, "transaction", "flink-fraud-group"),
        WatermarkStrategy.noWatermarks(),
        "transaction source"
      )
        .flatMap{
          json =>
            try{
              Some(JsonParser.parse[Transaction](json))
            } catch {
              case e: Exception =>
                logger.error(s"Bad message consumed from topic: $json", e)
                None
            }
        }
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
            .forBoundedOutOfOrderness[Transaction](Duration.ofSeconds(10))
            .withTimestampAssigner(new SerializableTimestampAssigner[Transaction] {
              override def extractTimestamp(t: Transaction, l: Long): Long = t.timestamp
            })
        )*/
        .fromElements(
        Transaction("user1", 1200, 1000L, 2.333333, 48.866667),
        Transaction("user1", 1300, 50000L, 2.333333, 48.866667), // within 1 min → fraud
        Transaction("user2", 200, 2000L, 2.333333, 48.866667),
        Transaction("user2", 5000, 9000000L, 134.379711, 34.886306),
        Transaction("user1", 1500, 200000L, 2.333333, 48.866667) // outside window
      )
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
            .forBoundedOutOfOrderness[Transaction](Duration.ofSeconds(10))
            .withTimestampAssigner(new SerializableTimestampAssigner[Transaction] {
              override def extractTimestamp(t: Transaction, l: Long): Long = t.timestamp
            } )
        )

    streamTx
      .keyBy(_.userId)
      .process(new FraudDetector)
      .map(_.toJson)
      //.sinkTo(KafkaSinkUtils.sink(KAFKA_BOOTSTRAP_SERVER_EXTERNAL, "fraud-alerts"))
      .print()

    env.execute("Flink Fraud Detection")

  }
}
