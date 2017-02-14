/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient

import java.io.StringWriter
import java.lang.reflect.{ParameterizedType, Type}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{ObjectMapper, PropertyNamingStrategy}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * Utility object for JSON parsing methods.
  */
object JsonUtils {
  var optObjectMapper: Option[ObjectMapper] = None

  // As of Jackson 2.4, we do not need a custom naming strategy - Jackson skips renaming on explicitly annotated fields
  private lazy val defaultObjectMapper = new ObjectMapper().registerModule(new DefaultScalaModule())
    .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)

  /**
    * Compares two [[com.fasterxml.jackson.databind.JsonNode]] for equality.
    * @param lhs first JSON string to be converted to JsonNode
    * @param rhs second JSON string to be converted to JsonNode
    * @param objectMapper [[com.fasterxml.jackson.databind.ObjectMapper]] to map JSON string to JsonNode
    * @return Boolean whether two JsonNodes are equal
    */
  def equals(lhs: String, rhs: String)(implicit objectMapper: ObjectMapper = optObjectMapper.getOrElse(defaultObjectMapper)): Boolean = {
    objectMapper.readTree(lhs) equals objectMapper.readTree(rhs)
  }

  /**
    * Returns JSON string from a generic value.
    * @param value object to be converted to JSON
    * @param objectMapper [[com.fasterxml.jackson.databind.ObjectMapper]] to map an object to JSON
    * @tparam T generic type
    * @return String of JSON
    */
  def toJson[T](value: T)(implicit objectMapper: ObjectMapper = optObjectMapper.getOrElse(defaultObjectMapper)): String = {
    val writer = new StringWriter
    objectMapper.writeValue(writer, value)
    writer.toString
  }

  /**
    * Returns object of type T from a JSON string.
    * @param value JSON string to be converted to type T
    * @param objectMapper [[com.fasterxml.jackson.databind.ObjectMapper]] to map a JSON string to an object
    * @param m implicit Manifest
    * @tparam T generic type
    * @return T instance representation of JSON
    */
  def fromJson[T](value: String)(implicit objectMapper: ObjectMapper = optObjectMapper.getOrElse(defaultObjectMapper), m: Manifest[T]): T = {
    objectMapper.readValue(value, typeReference[T])
  }

  /**
    * Returns a TypeReference from manifest.
    * @tparam T manifest object
    * @return TypeReference of type T
    */
  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType: Type = typeFromManifest(manifest[T])
  }

  /**
    * Returns a Type from manifest runtimeClass or creates new ParameterizedType.
    * @param m manifest object
    * @return Type
    */
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
}
