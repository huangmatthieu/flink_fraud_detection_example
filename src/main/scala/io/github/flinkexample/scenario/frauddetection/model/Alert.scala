package io.github.flinkexample.scenario.frauddetection.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


case class Alert(
                userId: String,
                message: String,
                lastTransaction: Transaction
                ) {
  def toJson: String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(this)
  }
}
