package io.github.flinkexample.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.reflect.ClassTag

object JsonParser {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def parse[T](json: String)(implicit ct: ClassTag[T]): T =
    mapper.readValue(json, ct.runtimeClass.asInstanceOf[Class[T]])

}
