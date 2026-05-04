package io.github.flinkexample.frauddetection.model

case class Alert(
                userId: String,
                message: String
                ) {
  def toJson: String =
    s"""{"userId":"$userId"; "message": "$message"}"""
}
