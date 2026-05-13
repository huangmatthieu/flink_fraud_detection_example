package io.github.flinkexample.scenario.topmovie.model

case class MovieCount(
                       movieId: String,
                       count: Long,
                       windowEnd: Long
                     )
