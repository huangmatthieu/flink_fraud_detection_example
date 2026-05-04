package io.github.flinkexample.frauddetection.sink

import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.kafka.clients.producer.ProducerConfig

object KafkaSinkUtils {

  def sink(bootstrapServer: String, topic: String): KafkaSink[String] =
    KafkaSink
      .builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic(topic)
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
      .setProperty(ProducerConfig.ACKS_CONFIG, "all")
      .build()


}
