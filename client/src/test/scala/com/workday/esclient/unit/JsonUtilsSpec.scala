package com.workday.esclient.unit

import com.fasterxml.jackson.databind.ObjectMapper

class JsonUtilsSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll
  with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar {

  val mapper= new ObjectMapper()
  /*
  behavior of "#mapToJson"
  it should "convert Map[String,Int] to Map[String,Json]" in {
    val map: Map[String, Int] = Map[String, Int]("key"->1)
    val expectedMap: Map[String, Json] = Map[String, Json]("key"->1.asJson)
    val mapJson = JsonUtils.mapToJson(map)
    mapJson shouldEqual expectedMap
  }

  it should "convert Map[String,String] to Map[String,Json]" in {
    val map: Map[String, String] = Map[String, String]("key"->"value")
    val expectedMap: Map[String, Json] = Map[String, Json]("key"->"value".asJson)
    val mapJson = JsonUtils.mapToJson(map)
    mapJson shouldEqual expectedMap
  }

  it should "convert Map[String,Any] to Map[String,Json]" in {
    val map: Map[String, Any] = Map[String, Any]("key"->Map[String, Int]("secondKey"->1))
    val expectedMap: Map[String, Json] = Map[String, Json]("key"->Map[String, Int]("secondKey"->1).asJson)
    val mapJson = JsonUtils.mapToJson(map)
    mapJson shouldEqual expectedMap
  }
  */
}