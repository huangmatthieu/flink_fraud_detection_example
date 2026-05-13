package io.github.flinkexample.scenario.frauddetection.transformations

import io.github.flinkexample.scenario.frauddetection.model.{Alert, Transaction}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration


class FraudDetector
  extends KeyedProcessFunction[String, Transaction, Alert] {

  private var lastTxState: ValueState[Transaction] = null

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[Transaction](
      "lastHighTx",
      classOf[Transaction]
    )

    val ttlConfig = StateTtlConfig
      // event time older than 1 hr are removed
      .newBuilder(Time.hours(9))
      .build()

    descriptor.enableTimeToLive(ttlConfig)

    lastTxState = getRuntimeContext.getState(descriptor)
  }

  override def processElement(
                               tx: Transaction,
                               ctx: KeyedProcessFunction[String, Transaction, Alert]#Context,
                               out: Collector[Alert]
                             ): Unit = {

    val lastTx = lastTxState.value

    if(lastTx != null) {

      if (tx.amount > 1000) {

        val lastTime = lastTx.timestamp

        if (lastTime != 0 && (tx.timestamp - lastTime) < 60000) {
          // Fraud detected
          out.collect(Alert(tx.userId, "Suspicious: 2 high transactions < 1 min", tx))
        }
      }

      val distanceKm = haversine(
        lastTx.latitude,
        lastTx.longitude,
        tx.latitude,
        tx.longitude
      )

      val timeDiffHours =
        (tx.timestamp - lastTx.timestamp).toDouble / 3600000.0

      if (timeDiffHours > 0) {
        val speed = distanceKm / timeDiffHours

        if (speed > 900) // threshold (km/h)
          out.collect(Alert(tx.userId, f"Impossible travel detected: speed=$speed%.2f km/h", tx))
      }
    }
      // update state
      lastTxState.update(tx)
  }

  // Haversine formula
  def haversine(
                 lat1: Double,
                 lon1: Double,
                 lat2: Double,
                 lon2: Double
               ): Double = {

    val R = 6371 // Earth radius in km

    val dLat = Math.toRadians(lat2 - lat1)
    val dLon = Math.toRadians(lon2 - lon1)

    val a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(Math.toRadians(lat1)) *
          Math.cos(Math.toRadians(lat2)) *
          Math.sin(dLon / 2) * Math.sin(dLon / 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    R * c
  }

}
