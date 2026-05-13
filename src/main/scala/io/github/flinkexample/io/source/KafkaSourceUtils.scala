package io.github.flinkexample.io.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

object KafkaSourceUtils {

  def source(bootstrapServer: String, topic: String, groupId: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setTopics(topic)
      .setGroupId(groupId)
      //.setProperty("security.protocol","SASL_SSL")
      //.setProperty("ssl.truststore.location", "/etc/kafka/conf/truststore.jks")
      //.setProperty("ssl.truststore.password", "bigdataflink")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

  }

}
