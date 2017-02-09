package com.workday.esclient

import java.io.StringWriter
import java.lang.reflect.{ParameterizedType, Type}

import cats.syntax.either._
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{ObjectMapper, PropertyNamingStrategy}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe._
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._

object JsonUtils {
  var optObjectMapper: Option[ObjectMapper] = None

  // As of Jackson 2.4, we do not need a custom naming strategy - Jackson skips renaming on explicitly annotated fields
  private lazy val defaultObjectMapper = new ObjectMapper().registerModule(new DefaultScalaModule())
    .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)

  def equals(lhs: String, rhs: String)(implicit objectMapper: ObjectMapper = optObjectMapper.getOrElse(defaultObjectMapper)): Boolean = {
    objectMapper.readTree(lhs) equals objectMapper.readTree(rhs)
  }

  def toJson[T](value: T)(implicit objectMapper: ObjectMapper = optObjectMapper.getOrElse(defaultObjectMapper)): String = {
    val writer = new StringWriter
    objectMapper.writeValue(writer, value)
    writer.toString
  }

  def fromJson[T](value: String)(implicit objectMapper: ObjectMapper = optObjectMapper.getOrElse(defaultObjectMapper), m: Manifest[T]): T = {
    objectMapper.readValue(value, typeReference[T])
  }

  def rawJson(value: String)(implicit objectMapper: ObjectMapper = optObjectMapper.getOrElse(defaultObjectMapper)): String = {
    value
  }

  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType: Type = typeFromManifest(manifest[T])
  }

  private[this] def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) {
      m.runtimeClass
    } else new ParameterizedType {
      def getRawType: Type = m.runtimeClass

      def getActualTypeArguments: Array[Type] = m.typeArguments.map(typeFromManifest).toArray

      // $COVERAGE-OFF$
      //  scalastyle:off
      def getOwnerType: Type = null

      //  scalastyle:on
      // $COVERAGE-ON$
    }
  }

  //TODO: replace jackson wrapper with circe wrapper.
  /*
  def mapToJson(rawValue: Map[String, Any]): Map[String, Json] = {
    var value = Map[String, Json]()
    for ((k,v) <- rawValue) {
      val nv = v match {
        case int: Int => int.asJson
        case string: String => string.asJson
        case _ => {
          try {
            val vAsMap = v.asInstanceOf[Map[String,Any]]
            mapToJson(vAsMap).asJson
          } catch {
            case _: Throwable => Json.Null
          }
        }
      }
      value = value + (k -> nv)
    }
    return value
  }
  */
}
