package io.github.flinkexample.scenario.topmovie.transformations

import io.github.flinkexample.scenario.topmovie.model.MovieCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.mutable

class TopNMovies(topSize: Int) extends KeyedProcessFunction[Long, MovieCount, String] {

  private var movieState:
    ListState[MovieCount] = _

  override def open(
                     parameters: org.apache.flink.configuration.Configuration
                   ): Unit = {

    val descriptor =
      new ListStateDescriptor[MovieCount](
        "movieState",
        classOf[MovieCount]
      )

    movieState =
      getRuntimeContext.getListState(descriptor)
  }

  override def processElement(
                               value: MovieCount,
                               ctx: KeyedProcessFunction[Long, MovieCount, String]#Context,
                               out: Collector[String]
                             ): Unit = {

    movieState.add(value)

    // timer at end of window
    ctx.timerService()
      .registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[Long, MovieCount, String]#OnTimerContext,
                        out: Collector[String]
                      ): Unit = {

    val allMovies = movieState.get().iterator().asScala.toList

    movieState.clear()

    val sortedMovies = allMovies.sortBy(-_.count).take(topSize)

    val result = new mutable.StringBuilder

    result.append("\n====================================\n")
    result.append(s"Top $topSize Movies\n")
    result.append("====================================\n")

    for (i <- sortedMovies.indices) {
      val current = sortedMovies(i)

      result.append(
        s"${i + 1}. Movie=${current.movieId} " +
          s"Views=${current.count}\n"
      )
    }

    out.collect(result.toString())
  }
}


