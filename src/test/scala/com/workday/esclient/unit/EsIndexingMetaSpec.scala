package com.workday.esclient.unit

import com.google.gson.{Gson, JsonObject}
import com.workday.esclient._
import com.workday.esclient.unit.EsClientSpec.EsClientWithMockedEs
import io.searchbox.client.JestClient
import io.searchbox.indices.{CloseIndex, CreateIndex, DeleteIndex}
import io.searchbox.indices.mapping.GetMapping
import org.mockito.Matchers.any
import org.mockito.Mockito.verify

class EsIndexingMetaSpec extends EsClientSpec {

  val TEST_INDEX_NAME = "index"
  val TEST_INDEX_2 = "index_2"
  val TEST_TYPE = "test_type"
  val TEST_TYPE_2 = "test_type_2"
  val TEST_TYPE_3 = "test_type_3"
  val TEST_FIELD = "test_field"
  val TEST_FIELD_2 = "test_field_2"

  val jsonFixture: String = {
    val responseMap = Map(TEST_INDEX_NAME -> Map("mappings"->
      Map(TEST_TYPE -> Map("properties" -> Map(TEST_FIELD -> Map("type" -> "number"))),
        TEST_TYPE_2 -> Map("properties" -> Map(TEST_FIELD_2 -> Map("type" -> "string"))))))
    JsonUtils.toJson(responseMap)
  }

  val jsonFixtureWithMultipleIndicies: String = {
    val responseMap = Map(TEST_INDEX_2 -> Map("mappings"-> Map(TEST_TYPE_3 -> Map("properties" -> Map(TEST_FIELD -> Map("type" -> "number"))))))
    JsonUtils.toJson(responseMap)
  }

  behavior of "#createIndex"
  it should "create index" in {
    val mockJestClient = mock[JestClient]
    val response = Acknowledgement_1_7(false)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenCreateIndex(JsonUtils.toJson(response))

    esClient.createIndex("").get shouldEqual response
    verify(mockJestClient).execute(any(classOf[CreateIndex]))
  }

  it should "create index with custom setting" in {
    val mockJestClient = mock[JestClient]
    val response = Acknowledgement_1_7(false)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenCreateIndex(JsonUtils.toJson(response))

    esClient.createIndex("", Option(Map("some_setting" -> "foo"))).get shouldEqual response
    verify(mockJestClient).execute(any(classOf[CreateIndex]))
  }

  "#deleteIndex" should "execute a DeleteIndex action and pass on the response" in {
    val mockJestClient = mock[JestClient]
    val response = Acknowledgement_1_7(true)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.deleteIndex("").get shouldEqual response
    verify(mockJestClient).execute(any(classOf[DeleteIndex]))
  }

  "#closeIndex" should "closeIndex" in {
    val mockJestClient = mock[JestClient]
    val response = Acknowledgement_1_7(false)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.closeIndex("").get shouldEqual response
    verify(mockJestClient).execute(any(classOf[CloseIndex]))
  }

  behavior of "#getMappingsForIndex"
  it should "return the mappings for the given index" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(jsonFixture)

    esClient.getMappingsForIndex(TEST_INDEX_NAME).get shouldEqual
      IndexMappings(Map(
        TEST_TYPE -> IndexTypeMappings(Map(TEST_FIELD -> IndexFieldProperties("number"))),
        TEST_TYPE_2 -> IndexTypeMappings(Map(TEST_FIELD_2 -> IndexFieldProperties("string")))
      ))
    verify(mockJestClient).execute(any(classOf[GetMapping]))
  }

  behavior of "#getMappingsForIndex(index, type)"
  it should "return the mappings for the given index and type name" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(jsonFixture)

    esClient.getMappingsForIndex(TEST_INDEX_NAME, TEST_TYPE).get shouldEqual
      IndexTypeMappings(Map(TEST_FIELD -> IndexFieldProperties("number")))
    verify(mockJestClient).execute(any(classOf[GetMapping]))
  }

  behavior of "#getMappingsForIndices"
  it should "return the mappings for the given index" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(jsonFixture)

    esClient.getMappingsForIndices(Seq(TEST_INDEX_NAME), Seq(TEST_TYPE, TEST_TYPE_2)).get shouldEqual
      Map(TEST_INDEX_NAME -> IndexMappings(Map(
        TEST_TYPE -> IndexTypeMappings(Map(TEST_FIELD -> IndexFieldProperties("number"))),
        TEST_TYPE_2 -> IndexTypeMappings(Map(TEST_FIELD_2 -> IndexFieldProperties("string")))
      )))
    verify(mockJestClient).execute(any(classOf[GetMapping]))
  }

  it should "return the mappings for the given index when duplicated" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(jsonFixture)

    esClient.getMappingsForIndices(Seq(TEST_INDEX_NAME, TEST_INDEX_NAME), Seq(TEST_TYPE, TEST_TYPE_2)).get shouldEqual
      Map(TEST_INDEX_NAME -> IndexMappings(Map(
        TEST_TYPE -> IndexTypeMappings(Map(TEST_FIELD -> IndexFieldProperties("number"))),
        TEST_TYPE_2 -> IndexTypeMappings(Map(TEST_FIELD_2 -> IndexFieldProperties("string")))
      )))
    verify(mockJestClient).execute(any(classOf[GetMapping]))
  }

  it should "return the mappings for the given indices and fields" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(jsonFixtureWithMultipleIndicies)
    esClient.getMappingsForIndices(Seq(TEST_INDEX_NAME, TEST_INDEX_2), Seq(TEST_TYPE_3)).get shouldEqual
      Map(TEST_INDEX_2 -> IndexMappings(Map(
        TEST_TYPE_3 -> IndexTypeMappings(Map(TEST_FIELD -> IndexFieldProperties("number")))
      )))
    verify(mockJestClient).execute(any(classOf[GetMapping]))
  }

  behavior of "#buildCreateIndex"
  it should "create a jest create index action" in {
    val esClient = new EsClient(mock[JestClient])
    val idx = esClient.buildCreateIndex("1")
    val emptyJson = new JsonObject
    idx.getData(new Gson()) shouldEqual emptyJson.toString
  }

  it should "create a jest create index action with custom settings" in {
    val esClient = new EsClient(mock[JestClient])
    val idx = esClient.buildCreateIndex("1", Option(Map("some_key" -> "some_value")))
    val emptyJson = new JsonObject
    idx.getData(new Gson()) shouldEqual JsonUtils.toJson(Map("some_key" -> "some_value"))
  }
}
