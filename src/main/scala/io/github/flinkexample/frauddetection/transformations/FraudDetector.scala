package io.github.flinkexample.frauddetection.transformations

import io.github.flinkexample.frauddetection.model.{Alert, Transaction}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration


class FraudDetector
  extends KeyedProcessFunction[String, Transaction, Alert] {

  private var lastHighTx: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[Long](
      "lastHighTx",
      classOf[Long]
    )

    val ttlConfig = StateTtlConfig
      // event time older than 1 hr are removed
      .newBuilder(Time.hours(1))
      .build()

    descriptor.enableTimeToLive(ttlConfig)

    lastHighTx = getRuntimeContext.getState(descriptor)
  }

  override def processElement(
                               tx: Transaction,
                               ctx: KeyedProcessFunction[String, Transaction, Alert]#Context,
                               out: Collector[Alert]
                             ): Unit = {

    if (tx.amount > 1000) {

      val lastTime = lastHighTx.value()

      if (lastTime != 0 && (tx.timestamp - lastTime) < 60000) {
        // Fraud detected
        out.collect(Alert(tx.userId, "Suspicious: 2 high transactions < 1 min"))
      }

      // update state
      lastHighTx.update(tx.timestamp)
    }
  }
}
