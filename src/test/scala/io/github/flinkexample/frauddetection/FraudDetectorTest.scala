package io.github.flinkexample.frauddetection

import io.github.flinkexample.frauddetection.model.{Alert, Transaction}
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.junit.Test
import org.junit.Assert._
import io.github.flinkexample.frauddetection.transformations.FraudDetector

class FraudDetectorTest {

  @Test
  def testFraudDetection(): Unit = {

    val function = new FraudDetector

    val harness =
      new KeyedOneInputStreamOperatorTestHarness[
        String, Transaction, Alert
      ](
        new KeyedProcessOperator(function),
        (tx: Transaction) => tx.userId,
        org.apache.flink.api.common.typeinfo.Types.STRING
      )

    harness.open()

    harness.processElement(
      Transaction("user1", 1200, 1000L),
      1000L
    )

    harness.processElement(
      Transaction("user1", 1300, 50000L),
      50000L
    )

    val result = harness.getOutput

    assertFalse(result.isEmpty)

    val alert = result.poll().asInstanceOf[StreamRecord[Alert]].getValue

    assertEquals("user1", alert.userId)

  }
}
