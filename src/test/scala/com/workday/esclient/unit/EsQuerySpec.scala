package com.workday.esclient.unit

import com.google.gson.{Gson, JsonObject}
import com.workday.esclient._
import com.workday.esclient.unit.EsClientSpec.{EsClientWithMockedEs, toJsonObject}
import io.searchbox.client.JestClient
import io.searchbox.core.{CountResult, Search}
import org.mockito.Matchers.any
import org.mockito.Mockito.{verifyZeroInteractions, when}

class EsQuerySpec extends EsClientSpec {

  val emptyAggregation = new JsonObject()
  val noResultResponse = EsSearchResponse(0, SearchHits(0, None, Nil), emptyAggregation)

  behavior of "#search"
  it should "search" in {
    val response = EsSearchResponse(77, SearchHits(1, Some(0.0), Seq(SearchHit("", "", "", Some(0.0), """{"prop":"value"}""", None, None))), emptyAggregation)
    val esResponseJson = """{"took": 77, "hits": { "total": 1, "max_score": 0.0, "hits": [{ "_index": "", "_type": "", "_id": "", "_score": 0.0, "_source": { "prop": "value" } }] } }"""

    val esClient = new EsClientWithMockedEs().whenSearch(esResponseJson)

    esClient.search("", "").get shouldEqual response
    esClient.search("", "", "", Map.empty).get shouldEqual response
  }

  it should "return successfully when max_score is null from ES" in {
    val esResponseJson =  s"""{"took": 0, "hits": { "total": 0, "max_score": null, "hits": [] } }"""

    val esClient = new EsClientWithMockedEs().whenSearch(esResponseJson)

    esClient.search("", "").get shouldEqual noResultResponse
  }

  it should "return successfully when max_score is missing from ES" in {
    val esResponseJson =   s"""{"took": 0, "hits": { "total": 0, "hits": [] } }"""

    val esClient = new EsClientWithMockedEs().whenSearch(esResponseJson)

    esClient.search("", "").get shouldEqual noResultResponse
  }

  it should "include aggregations in search results" in {
    val aggregation = new JsonObject()
    aggregation.addProperty("count", 1)
    val response = EsSearchResponse(0, SearchHits(1, Some(0.5), Seq(SearchHit("", "", "", Some(0.5), """{"prop":"value"}""", None, None))), aggregation)
    val esResponseJson =  s"""{
                              |  "took": 0,
                              |  "hits": {
                              |    "total": 1,
                              |    "max_score": 0.5,
                              |    "hits": [{
                              |      "_index": "",
                              |      "_type": "",
                              |      "_id": "",
                              |      "_score": 0.5,
                              |      "_source": { "prop": "value" }
                              |    }]
                              |  },
                              |  "aggregations": $aggregation
                              |}""".stripMargin

    val esClient = new EsClientWithMockedEs().whenSearch(esResponseJson)
    esClient.search("", "").get shouldEqual response
  }

  it should "include matched queries in search results" in {
    val matchedFields = Some(Seq("queryName1", "queryName2"))
    val response = EsSearchResponse(77, SearchHits(1, Some(0.0), Seq(SearchHit("", "", "", Some(0.0), "", matchedFields, None))), emptyAggregation)
    val esResponseJson =
      """{
         |  "took": 77,
         |  "hits": {
         |    "total": 1,
         |    "max_score": 0.0,
         |    "hits": [{
         |      "_id": "",
         |      "_score": 0.0,
         |      "matched_queries": [
         |        "queryName1",
         |        "queryName2"
         |      ]
         |    }]
         |  }
         |}""".stripMargin

    val esClient = new EsClientWithMockedEs().whenSearch(esResponseJson)
    esClient.search("", "").get shouldEqual response
  }

  it should "include matched queries in search results but remove named query delimiter from query name" in {
    val matchedFields = Some(Seq("queryName1", "queryName2"))
    val response = EsSearchResponse(77, SearchHits(1, Some(0.0), Seq(SearchHit("", "", "", Some(0.0), "", matchedFields, None))), emptyAggregation)
    val esResponseJson =
      """{
        |  "took": 77,
        |  "hits": {
        |    "total": 1,
        |    "max_score": 0.0,
        |    "hits": [{
        |      "_id": "",
        |      "_score": 0.0,
        |      "matched_queries": [
        |        "queryName1##whatever",
        |        "queryName2##whatever2"
        |      ]
        |    }]
        |  }
        |}""".stripMargin

    val esClient = new EsClientWithMockedEs().whenSearch(esResponseJson)
    esClient.search("", "").get shouldEqual response
  }

  behavior of "#get"
  it should "get" in {
    val response = GetResponse_1_7("", "", "", 0, None, Some("""{"prop":"value"}"""), true)
    val esResponseJson = """{ "_index": "", "_type": "", "_id": "", "_version": 0, "found": true, "_source": { "prop": "value" } }"""

    val esClient = new EsClientWithMockedEs().whenGet(esResponseJson)

    esClient.get("", "").get.source shouldEqual response.source
  }

  it should "foundVersion should return version when found is true" in {
    val esResponseJson = """{ "_index": "", "_type": "", "_id": "", "_version": 0, "found": true, "_source": { "prop": "value" } }"""

    val esClient = new EsClientWithMockedEs().whenGet(esResponseJson)

    esClient.get("", "").get.foundVersion shouldEqual Some(0)
  }

  it should "foundVersion should return nothing when found is false" in {
    val esResponseJson = """{ "_index": "", "_type": "", "_id": "", "_version": 0, "found": false, "_source": { "prop": "value" } }"""

    val esClient = new EsClientWithMockedEs().whenGet(esResponseJson)

    esClient.get("", "").get.foundVersion shouldEqual None
  }

  it should "return EsInvalidResponse if the response looks bad" in {
    val esClient = new EsClientWithMockedEs().whenGet("Unable to Parse JSON into the given Class.", expectJsonError = true)

    val result = esClient.get("", "")
    result shouldEqual EsInvalidResponse("Unable to Parse JSON into the given Class.")
  }

  it should "return EsError when there's an error" in {
    val response = EsError_1_7("This is an error", 400)
    val esClient = new EsClientWithMockedEs().whenGet(JsonUtils.toJson(response))

    val result = esClient.get("", "")
    result shouldEqual response
    val ex = the[NoSuchElementException] thrownBy { result.get }
    ex.getMessage should include ("This is an error")
  }

  behavior of "#multiGet"
  it should "not interact with ElasticSearch when given an empty list of IDs" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClient(mockJestClient)
    esClient.multiGet("", "", Nil).get shouldEqual MultiGetResponse(Nil)
    verifyZeroInteractions(mockJestClient)
  }

  it should "get multiple documents" in {
    val response = MultiGetResponse(
      Seq(
        GetResponse_1_7("", "", "a", 0, None, Some("""{"prop":"value"}"""), true),
        GetResponse_1_7("", "", "b", 0, None, Some("""{"prop":"value"}"""), true)
      )
    )
    val esResponseJson =
      """{ "docs":
        | [
        |  { "_index": "", "_type": "", "_id": "a", "_version": 0, "found": true, "_source": {"prop":"value"} },
        |  { "_index": "", "_type": "", "_id": "b", "_version": 0, "found": true, "_source": {"prop":"value"} }
        | ]
        |}""".stripMargin

    val esClient = new EsClientWithMockedEs().whenMultiGet(esResponseJson)


    esClient.multiGet("", "", Seq("a", "b")).get.docs.map(_.source) shouldEqual response.docs.map(_.source)
  }

  it should "get multiple documents with requested field" in {
    val response = MultiGetResponse(
      Seq(
        GetResponse_1_7("", "", "a", 0, None, Some("""{"prop":"value"}"""), true),
        GetResponse_1_7("", "", "b", 0, None, Some("""{"prop":"value"}"""), true)
      )
    )
    val esResponseJson =
      """{ "docs":
        | [
        |  { "_index": "", "_type": "", "_id": "a", "_version": 0, "found": true, "_source": {"prop":"value"} },
        |  { "_index": "", "_type": "", "_id": "b", "_version": 0, "found": true, "_source": {"prop":"value"} }
        | ]
        |}""".stripMargin

    val esClient = new EsClientWithMockedEs().whenMultiGet(esResponseJson)


    esClient.multiGetSourceField("", "", Seq("a", "b"), "prop").get.docs.map(_.source) shouldEqual response.docs.map(_.source)
  }

  it should "parse get responses with errors" in {
    val response = MultiGetResponse(
      Seq(
        GetResponse_1_7("", "", "a", 0, None, None, false, Some("could not find index")),
        GetResponse_1_7("", "", "b", 0, None, Some("""{"prop":"value"}"""), true)
      )
    )
    val esResponseJson =
      """{ "docs":
        | [
        |  { "_index": "", "_type": "", "_id": "a", "_version": 0, "found": false, "error": "could not find index" },
        |  { "_index": "", "_type": "", "_id": "b", "_version": 0, "found": true, "_source": {"prop":"value"} }
        | ]
        |}""".stripMargin

    val esClient = new EsClientWithMockedEs().whenMultiGet(esResponseJson)
    val esResponse = esClient.multiGet("", "", Seq("a", "b")).get
    esResponse.docs.map(_.source) shouldEqual response.docs.map(_.source)
  }

  behavior of "#count"
  it should "count" in {
    val mockJestResult = mock[CountResult]
    val mockJestClient = mock[JestClient]
    when(mockJestClient.execute(any())).thenReturn(mockJestResult)
    when(mockJestResult.getCount()).thenReturn(1.0)
    when(mockJestResult.getJsonObject).thenReturn(toJsonObject("""{ "count": "1.0" }"""))
    val esClient = new EsClient(mockJestClient)
    esClient.count("").get shouldEqual 1
  }

  it should "count with type" in {
    val mockJestResult = mock[CountResult]
    val mockJestClient = mock[JestClient]
    when(mockJestClient.execute(any())).thenReturn(mockJestResult)
    when(mockJestResult.getCount()).thenReturn(1.0)
    when(mockJestResult.getJsonObject).thenReturn(toJsonObject("""{ "count": "1.0" }"""))
    val esClient = new EsClient(mockJestClient)
    esClient.count("", Some("type")).get shouldEqual 1
  }

  behavior of "#getSearchHitsCount"
  it should "get search hits count" in {
    val response = EsSearchResponse(77, SearchHits(1, Some(0.0), Seq(SearchHit("", "", "", Some(0.0), """{"prop":"value"}""", None, None))), emptyAggregation)
    val esResponseJson = """{"took": 77, "hits": { "total": 123, "max_score": 0.0, "hits": [{ "_index": "", "_type": "", "_id": "", "_score": 0.0, "_source": { "prop": "value" } }] } }"""

    val esClient = new EsClientWithMockedEs().whenSearch(esResponseJson)

    esClient.getSearchHitsCount("", "") shouldEqual 123
  }

  "#buildSearchAction" should "create a jest search action" in {
    val esClient = new EsClient(mock[JestClient])
    val query = "{}"
    val index = "index"
    val typeName = "type"

    def verify(search: Search, typeName: String): Unit = {
      JsonUtils.equals(search.getData(new Gson()).toString, query) shouldEqual true
      search.getIndex shouldEqual index
      search.getType shouldEqual typeName
    }

    val search = esClient.buildSearchAction(query, index)
    verify(search, "")

    val searchWithTypeName = esClient.buildSearchAction(query, index, typeName)
    verify(searchWithTypeName, typeName)

    val searchWithParams = esClient.buildSearchAction(query, index, params = Map("fields" -> Seq("1$2")))
    searchWithParams.getParameter("fields").contains(Seq("1$2")) shouldEqual true
  }

  "#buildGetAction" should "create a jest get action" in {
    val esClient = new EsClient(mock[JestClient])
    val get = esClient.buildGetAction("1", "2")
    get.getIndex shouldEqual "1"
    get.getId shouldEqual "2"
  }

  "#buildMultiGetAction" should "create a jest MultiGet action" in {
    val esClient = new EsClient(mock[JestClient])
    val multiGet = esClient.buildMultiGetAction("idx", "type", Seq("1", "2", "3"), Map("_source" -> false))
    val multiGetActionJson = """{"ids":["1","2","3"]}"""
    JsonUtils.equals(multiGet.getData(new Gson()).toString, multiGetActionJson) shouldEqual true
  }
}
