package io.github.flinkexample.scenario.topmovie

import io.github.flinkexample.scenario.topmovie.model.ViewEvent
import io.github.flinkexample.scenario.topmovie.transformations.{CountAgg, TopNMovies, WindowResultFunction}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import java.time.Duration

object TopMovieMain {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val events = env.fromElements(

      ViewEvent("u1", "movieA", 1000L),
      ViewEvent("u2", "movieA", 2000L),
      ViewEvent("u3", "movieB", 3000L),
      ViewEvent("u4", "movieA", 4000L),
      ViewEvent("u5", "movieC", 5000L),
      ViewEvent("u6", "movieB", 6000L),
      ViewEvent("u7", "movieB", 7000L),
      ViewEvent("u8", "movieC", 8000L),
      ViewEvent("u9", "movieC", 9000L),
      ViewEvent("u10", "movieC", 10000L)
    )

    // Watermark strategy
    val watermarkStrategy =
      WatermarkStrategy
        .forBoundedOutOfOrderness[ViewEvent](Duration.ofSeconds(5))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[ViewEvent] {
            override def extractTimestamp(
                                           element: ViewEvent,
                                           recordTimestamp: Long
                                         ): Long = element.timestamp
          }
        )

    val topMovies = events
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .keyBy(_.movieId)
      .window(
        SlidingEventTimeWindows.of(
          Time.seconds(5),
          Time.seconds(10)
        )
      )
      .aggregate(
        new CountAgg,
        new WindowResultFunction
      )
      .keyBy(_.windowEnd)
      .process(new TopNMovies(5))

    topMovies.print()

    env.execute("Flink Top Movie")

  }

}
