package com.workday.esclient.unit

import com.google.gson.{Gson, JsonObject}
import com.workday.esclient._
import io.searchbox.client.{JestClient, JestResult}

class JestUtilsSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll
  with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar  {

  behavior of "#toEsResult"
  it should "return an EsResponse for a successful result" in {
    val esClient = new EsClient(mock[JestClient])

    val jestResult = new JestResult(new Gson())
    val jsonString = """{"acknowledged": true}"""
    val jsonObject = new JsonObject()
    jsonObject.addProperty("acknowledged", true)
    jestResult.setJsonString(jsonString)
    jestResult.setJsonObject(jsonObject)

    val expectedResponse = EsResponse(Acknowledgement(true))

    esClient.toEsResult[Acknowledgement](jestResult) shouldEqual expectedResponse
  }

  it should "not break if jsonObject set, but jsonString not set" in {
    val esClient = new EsClient(mock[JestClient])

    val jestResult = new JestResult(new Gson())
    val jsonObject = new JsonObject()
    jsonObject.addProperty("acknowledged", true)
    jestResult.setJsonObject(jsonObject)

    val expectedResponse = EsResponse(Acknowledgement(true))

    esClient.toEsResult[Acknowledgement](jestResult) shouldEqual expectedResponse
  }

  it should "not break if neither jsonObject nor jsonString set" in {
    val esClient = new EsClient(mock[JestClient])
    val jestResult = new JestResult(new Gson())

    val expectedResponse = EsInvalidResponse("Unable to Parse JSON into the given class because result contains all NULL entries.")

    esClient.toEsResult[Any](jestResult) shouldEqual expectedResponse
  }

  it should "ignore errors if error field exists but allowError is true" in {
    val esClient = new EsClient(mock[JestClient])

    val jestResult = new JestResult(new Gson())
    val jsonString = """{"error": "get rekt"}"""
    val jsonObject = new JsonObject()
    jsonObject.addProperty("error", "get rekt")
    jestResult.setJsonString(jsonString)
    jestResult.setJsonObject(jsonObject)

    val expectedResponse = EsResponse(TestError("get rekt"))

    esClient.toEsResult[TestError](jestResult, allowError = true) shouldEqual expectedResponse
  }

  it should "return EsError if error field exists and allowError is false" in {
    val esClient = new EsClient(mock[JestClient])

    val jestResult = new JestResult(new Gson())
    val jsonString = """{"error": "get rekt"}"""
    val jsonObject = new JsonObject()
    jsonObject.addProperty("error", "get rekt")
    jestResult.setJsonString(jsonString)
    jestResult.setJsonObject(jsonObject)

    val expectedResponse = JsonUtils.fromJson[EsError](jsonString)

    esClient.toEsResult[TestError](jestResult) shouldEqual expectedResponse
  }

  it should "return EsInvalidResponse if JsonObject is empty, but jsonString is set to invalid JSON." in {
    val esClient = new EsClient(mock[JestClient])

    val jestResult = new JestResult(new Gson())
    val jsonString = "Bad Json"
    jestResult.setJsonString(jsonString)

    val expectedResponse = EsInvalidResponse("Unable to Parse JSON into the given Class.")

    esClient.toEsResult[Any](jestResult) shouldEqual expectedResponse
  }

  it should "return EsInvalidResponse if JsonObject is empty, but jsonString is set to empty string." in {
    val esClient = new EsClient(mock[JestClient])

    val jestResult = new JestResult(new Gson())
    val jsonString = ""
    jestResult.setJsonString(jsonString)

    val expectedResponse = EsInvalidResponse("Unable to Map JSON to the given Class.")

    esClient.toEsResult[Any](jestResult) shouldEqual expectedResponse
  }

  behavior of "#EsResult.map"
  it should "return a new result when original result is EsResponse" in {
    val result = EsResponse(30)
    result.map(_ + 1) shouldEqual EsResponse(31)
  }

  it should "return original result when result is EsInvalidResponse" in {
    val result: EsResult[Int] = EsInvalidResponse("error")
    result.map(_ + 1) shouldEqual result
  }

  it should "return original result when result is EsError" in {
    val result: EsResult[Int] = EsError("error", 0)
    result.map(_ + 1) shouldEqual result
  }
}

case class TestError(
  error: String
)
