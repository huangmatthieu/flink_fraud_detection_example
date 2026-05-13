package io.github.flinkexample.scenario.topmovie.model

case class ViewEvent(
                      userId: String,
                      movieId: String,
                      timestamp: Long
                    )
