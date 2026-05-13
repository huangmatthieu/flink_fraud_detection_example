package io.github.flinkexample.io.sink

import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.kafka.clients.producer.ProducerConfig

object KafkaSinkUtils {

  def sink(bootstrapServer: String, topic: String): KafkaSink[String] =
    KafkaSink
      .builder[String]()
      .setBootstrapServers(bootstrapServer)
      //.setProperty("security.protocol", "SASL_SSL")
      //.setProperty("ssl.truststore.location", "/etc/kafka/conf/truststore.jks")
      //.setProperty("ssl.truststore.password", "bigdataflink")
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
