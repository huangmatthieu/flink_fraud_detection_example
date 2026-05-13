package io.github.flinkexample.scenario.topmovie.transformations

import io.github.flinkexample.scenario.topmovie.model.ViewEvent
import org.apache.flink.api.common.functions.AggregateFunction

class CountAgg extends AggregateFunction[ViewEvent, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(
                    value: ViewEvent,
                    accumulator: Long
                  ): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b

}

