package io.github.flinkexample.scenario.topmovie.transformations

import io.github.flinkexample.scenario.topmovie.model.MovieCount
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector

class WindowResultFunction extends WindowFunction[Long, MovieCount, String, TimeWindow] {

  override def apply(
                        key: String,
                        window: TimeWindow,
                        elements: Iterable[Long],
                        out: Collector[MovieCount]): Unit = {
    val count = elements.iterator.next()

    out.collect(
      MovieCount(
        key,
        count,
        window.getEnd
      )
    )
  }
}
