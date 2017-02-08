package com.workday.esclient

import cats.syntax.either._
import io.circe._
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import shapeless.Lazy

//TODO: replace with circe wrapper
object JsonUtils {
  def equals(lhs: String, rhs: String): Boolean = {
    ???
  }

  def toJson[T](value: T): String = {
    //val outerJson: String = value.asJson.noSpaces
    //Can't use cursor until we decide the downfield class name we'll be working with.
    //val cursor: HCursor = outerJson.hcursor
    //val innerJson: Json = cursor.downField()
    //getting compile errors here so we might need a custom encoder
    //TODO: make custom encoder if can't fix compile error
    //value.asJson.noSpaces
    ???
  }

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

  def fromJson[T](value: String): T = {
    //val rawJson = parse(value).getOrElse(Json.Null)
    //TODO: make case classes
    //rawJson.as[T]
    ???
  }
}

abstract class Message() {}
case class StringMessage(x: String) {}
case class IntMessage(x: Int) {}