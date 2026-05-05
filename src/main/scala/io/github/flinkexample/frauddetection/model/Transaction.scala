package io.github.flinkexample.frauddetection.model

case class Transaction(
                 userId: String,
                 amount: Double,
                 timestamp: Long,
                 longitude: Double,
                 latitude: Double
                 )
