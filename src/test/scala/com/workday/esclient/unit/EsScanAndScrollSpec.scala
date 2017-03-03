package com.workday.esclient.unit

import com.google.gson.{Gson, JsonObject, JsonParser}
import com.workday.esclient._
import com.workday.esclient.unit.EsClientSpec.EsClientWithMockedEs
import io.searchbox.client.{JestClient, JestResult}
import io.searchbox.core.Search
import io.searchbox.params.Parameters
import org.mockito.Mockito.when

class EsScanAndScrollSpec extends EsClientSpec {

  val emptyAggregation = new JsonObject()
  val noResultResponse = EsSearchResponse(0, SearchHits(0, None, Nil), emptyAggregation)
  val defaultUnlimitedResultsScanAndScrollSize = 10000
  val unlimitedResultsDefaultParams = Map(EsNames.FilterPath -> EsNames.FilterPathParams, Parameters.SIZE -> defaultUnlimitedResultsScanAndScrollSize)

  behavior of "#unlimitedSearch"
  it should "search for unlimited results" in {
    val scrollId = "something"
    val scrollId2 = "something2"
    val scrollId3 = "something3"
    val expectedResponse = EsSearchResponse(117, SearchHits(1, Some(2.0), Seq(SearchHit("", "", "1", 1.0, "", Some(Seq("job title","summary")), None), SearchHit("", "", "2", 2.0, "", Some(Seq("job title","summary")), None))), emptyAggregation)
    val esResponseJson = s"""{"_scroll_id": $scrollId, "took": 77, "hits": { "total": 1, "max_score": 0.0, "hits": [{ "_id": "1", "_score": 1.0, "matched_queries": ["job title##","summary##"] }] } }"""
    val esResponseJson2 = s"""{"_scroll_id": $scrollId2, "took": 0, "hits": { "total": 1, "max_score": 2.0, "hits": [{ "_id": "2", "_score": 2.0, "matched_queries": ["job title##","summary##"] }] } }"""
    val esResponseJson3 = s"""{"_scroll_id": $scrollId3, "took": 40, "hits": { "total": 0, "max_score": 1.0 } }"""

    val esClient = new EsClientWithMockedEs().whenScrolledSearch(esResponseJson)
    esClient.mockJestResult(esResponseJson, esClient.buildScrolledSearchAction("", "", "", unlimitedResultsDefaultParams))
    esClient.mockJestResult(esResponseJson2, esClient.buildGetScrolledSearchResultsAction(scrollId))
    esClient.mockJestResult(esResponseJson3, esClient.buildGetScrolledSearchResultsAction(scrollId2))

    esClient.unlimitedSearch("", query = "", scanAndScrollSize = defaultUnlimitedResultsScanAndScrollSize).get shouldEqual expectedResponse
  }

  it should "search for unlimited results should result in an EsError" in {
    val scrollId = "something"
    val expectedResponse = EsError("ERROR", 1)
    val esResponseJson = s"""{"_scroll_id": $scrollId, "took": 77, "hits": { "total": 1, "max_score": 0.0, "hits": [{ "_id": "", "_score": 0.0 }] } }"""
    val esResponseJson2 = s"""{"error": "ERROR", "status": 1}"""

    val esClient = new EsClientWithMockedEs().whenScrolledSearch(esResponseJson)

    esClient.mockJestResult(esResponseJson, esClient.buildScrolledSearchAction("", "", "", unlimitedResultsDefaultParams))
    esClient.mockJestResult(esResponseJson2, esClient.buildGetScrolledSearchResultsAction(scrollId))

    esClient.unlimitedSearch("", query = "", scanAndScrollSize = defaultUnlimitedResultsScanAndScrollSize) shouldEqual expectedResponse
  }

  it should "search for unlimited results should result in an EsInvalidResponse" in {
    val scrollId = "something"

    val expectedResponse = EsInvalidResponse("Unable to Parse JSON into the given class because result contains all NULL entries.")

    val esResponseJson = s"""{"_scroll_id": $scrollId, "took": 77, "hits": { "total": 1, "max_score": 0.0, "hits": [{ "_id": "", "_score": 0.0 }] } }"""

    val esClient = new EsClientWithMockedEs().whenScrolledSearch(esResponseJson)

    esClient.mockJestResult(esResponseJson, esClient.buildScrolledSearchAction("", "", "", unlimitedResultsDefaultParams))
    when(esClient.jest.execute(esClient.buildGetScrolledSearchResultsAction(scrollId))).thenReturn(mock[JestResult])

    esClient.unlimitedSearch("", query = "", scanAndScrollSize = defaultUnlimitedResultsScanAndScrollSize) shouldEqual expectedResponse
  }

  it should "return successfully when max_score is missing from ES when getting unlimited results" in {
    val scrollId = "something"
    val esResponseJson =   s"""{"_scroll_id": $scrollId, "took": 0, "hits": { "total": 0, "hits": [] } }"""
    val esResponseJson2 = s"""{"_scroll_id": "something2", "took": 0, "hits": { "total": 0, "hits": [] } }"""

    val esClient = new EsClientWithMockedEs().whenScrolledSearch(esResponseJson)
    esClient.mockJestResult(esResponseJson, esClient.buildScrolledSearchAction("", "", "", unlimitedResultsDefaultParams))
    esClient.mockJestResult(esResponseJson2, esClient.buildGetScrolledSearchResultsAction(scrollId))

    esClient.unlimitedSearch("", query = "", scanAndScrollSize = defaultUnlimitedResultsScanAndScrollSize).get shouldEqual noResultResponse
  }

  it should "return successfully when max_score is null from ES when getting unlimited results" in {
    val scrollId = "something"
    val esResponseJson =  s"""{"_scroll_id": $scrollId, "took": 0, "hits": { "total": 0, "max_score": null } }"""
    val esResponseJson2 = s"""{"_scroll_id": "something2", "took": 0, "hits": { "total": 0, "max_score": null } }"""

    val esClient = new EsClientWithMockedEs().whenScrolledSearch(esResponseJson)
    esClient.mockJestResult(esResponseJson, esClient.buildScrolledSearchAction("", "", "", unlimitedResultsDefaultParams))
    esClient.mockJestResult(esResponseJson2, esClient.buildGetScrolledSearchResultsAction(scrollId))

    esClient.unlimitedSearch("", query = "", scanAndScrollSize = defaultUnlimitedResultsScanAndScrollSize).get shouldEqual noResultResponse
  }

  it should "include aggregations in search results when getting unlimited results" in {
    val scrollId = "something"
    val aggregation = new JsonObject();
    aggregation.addProperty("count", 1)
    val response = EsSearchResponse(77, SearchHits(1, Some(0.5), Seq(SearchHit("", "", "1", 0.5, "", None, None))), aggregation)
    val esResponseJson =  s"""{
                              |"_scroll_id": $scrollId,
                              |  "took": 0,
                              |  "hits": {
                              |    "total": 1,
                              |    "max_score": 0.5,
                              |    "hits": [{
                              |      "_id": "1",
                              |      "_score": 0.5
                              |    }]
                              |  },
                              |  "aggregations": $aggregation
                              |}""".stripMargin

    val esResponseJson2 = s"""{"_scroll_id": "scrollId2", "took": 77, "hits": { "total": 0, "max_score": 0.0 } }"""

    val esClient = new EsClientWithMockedEs().whenScrolledSearch(esResponseJson)
    esClient.mockJestResult(esResponseJson, esClient.buildScrolledSearchAction("", "", "", unlimitedResultsDefaultParams))
    esClient.mockJestResult(esResponseJson2, esClient.buildGetScrolledSearchResultsAction(scrollId))

    esClient.unlimitedSearch("", query = "", scanAndScrollSize = defaultUnlimitedResultsScanAndScrollSize).get shouldEqual response
  }

  behavior of "#createScrolledSearchIterator" // indirectly tests #createScrolledSearch
  it should "return an iterator of results" in {
    val esResponseJson =
      s"""{
          |   "_scroll_id": "scrollID",
          |   "took": 40,
          |   "timed_out": false,
          |   "_shards": {
          |      "total": 1,
          |      "successful": 1,
          |      "failed": 0
          |   },
          |   "hits": {
          |      "total": 12,
          |      "max_score": 0,
          |      "hits": []
          |   }
          |}
       """.stripMargin

    val esClient = new EsClientWithMockedEs().whenAny(esResponseJson)

    val response = esClient.createScrolledSearchIterator("indexName", "typeName", "bogusQuery")
    response.next().get shouldEqual SearchHits(12, Some(0.0), Nil)
  }

  it should "return a multi-page iterator of results" in {
    val esResponseJson =
      s"""{
          |   "_scroll_id": "scrollID1",
          |   "took": 40,
          |   "timed_out": false,
          |   "_shards": {
          |      "total": 1,
          |      "successful": 1,
          |      "failed": 0
          |   },
          |   "hits": {
          |      "total": 12,
          |      "max_score": 0,
          |      "hits": []
          |   }
          |}
       """.stripMargin
    val response1 =
      s"""{
          |   "_scroll_id": "scrollID2",
          |   "took": 40,
          |   "timed_out": false,
          |   "_shards": {
          |      "total": 1,
          |      "successful": 1,
          |      "failed": 0
          |   },
          |   "hits": {
          |      "total": 12,
          |      "max_score": 0,
          |      "hits": [{
          |        "_index": "",
          |        "_type": "",
          |        "_id": "",
          |        "_score": 0.0,
          |        "_source": { "prop": "value" },
          |        "_explanation": {
          |               "value": 0.08948636,
          |               "description": "product of:",
          |               "details": []
          |               }
          |      }]
          |   }
          |}
       """.stripMargin

    val response2 =
      s"""{
          |   "_scroll_id": "scrollID3",
          |   "took": 40,
          |   "timed_out": false,
          |   "_shards": {
          |      "total": 1,
          |      "successful": 1,
          |      "failed": 0
          |   },
          |   "hits": {
          |      "total": 12,
          |      "max_score": 0,
          |      "hits": []
          |   }
          |}
       """.stripMargin

    val esClient = new EsClientWithMockedEs()
    esClient.mockJestResult(esResponseJson, esClient.buildScrolledSearchAction("bogusQuery", "indexName", "typeName"))
    esClient.mockJestResult(response1, esClient.buildGetScrolledSearchResultsAction("scrollID1"))
    esClient.mockJestResult(response2, esClient.buildGetScrolledSearchResultsAction("scrollID2"))

    val response = esClient.createScrolledSearchIterator("indexName", "typeName", "bogusQuery").toSeq
    response.size shouldEqual 3
    response(0).get shouldEqual SearchHits(12, Some(0.0), Nil)
    response(1).get shouldEqual
      SearchHits(12, Some(0.0), Seq(SearchHit("", "", "", 0.0, """{"prop":"value"}""", None, Some("""{"value":0.08948636,"description":"product of:","details":[]}"""))))
    response(2).get shouldEqual SearchHits(12, Some(0.0), Nil)
  }

  behavior of "#getScrolledSearchResults"
  it should "return the first section of the scrolled search results" in {
    val esResponseJson =
      s"""{
          |   "_scroll_id": "scrollID",
          |   "took": 40,
          |   "timed_out": false,
          |   "_shards": {
          |      "total": 1,
          |      "successful": 1,
          |      "failed": 0
          |   },
          |   "hits": {
          |      "total": 12,
          |      "max_score": 0,
          |      "hits": [{
          |        "_index": "",
          |        "_type": "",
          |        "_id": "",
          |        "_score": 0.0,
          |        "_source": { "prop": "value" }
          |      }]
          |   }
          |}
       """.stripMargin

    val esClient = new EsClientWithMockedEs().whenAny(esResponseJson)

    val expectedResponse = ScanAndScrollResponse(40, SearchHits(12, Some(0.0), Seq(SearchHit("", "", "", 0, """{"prop":"value"}""", None, None))), new JsonObject, "scrollID")

    val response = esClient.getScrolledSearchResults("bogusScrollID")
    response.get shouldEqual expectedResponse
  }

  behavior of "#handleHitsInResult"
  val devOmsIndexName = "dev@oms"
  val docId = "1$2"
  val sid = "1$1"

  it should "process full docs" in {
    val resultJson = s"""{"hits": {"total": 1, "maxScore": 1.0, "hits": [{"_index": "$devOmsIndexName", "_type": "$sid", "_id": "$docId", "_score": 1.0, "_source": {}}]}}"""
    val resultJsonObject = new JsonParser().parse(resultJson).getAsJsonObject
    val esClient = new EsClientWithMockedEs()
    esClient.handleHitsInResult(resultJsonObject).hits.length shouldBe 1
  }

  it should "process no-source docs" in {
    val resultJson = s"""{"hits": {"total": 1, "maxScore": 1.0, "hits": [{"_index": "$devOmsIndexName", "_type": "$sid", "_id": "$docId", "_score": 1.0}]}}"""
    val resultJsonObject = new JsonParser().parse(resultJson).getAsJsonObject
    val esClient = new EsClientWithMockedEs()
    esClient.handleHitsInResult(resultJsonObject).hits.length shouldBe 1
  }

  behavior of "#accumulateScanAndScrollResponses"
  it should "return some errors when accumulating unlimited if it gets an EsError in one of the pages" in {
    val response = EsError("error", 1)
    val expectedBadRequestResponse = EsInvalidResponse("error")
    val esResponseJson = s"""{"error" : "error", "status" : 1 }"""

    val esClient = new EsClientWithMockedEs().whenScrolledSearch(esResponseJson)

    esClient.accumulateScanAndScrollResponses(response, EsResponse(ScanAndScrollResponse(0, SearchHits(0, Option(0), Nil), new JsonObject, ""))) shouldEqual response
    esClient.accumulateScanAndScrollResponses(expectedBadRequestResponse, EsResponse(ScanAndScrollResponse(0, SearchHits(0, Option(0), Nil), new JsonObject, ""))) shouldEqual expectedBadRequestResponse
  }

  behavior of "#getScrolledSearchHitsIterator"
  it should "return an iterator of results hits with errors" in {
    val esClient = new EsClientWithMockedEs()
    esClient.getScrolledSearchHitsIterator(EsError("error", 1)).toSeq shouldEqual Seq(EsError("error", 1))
    esClient.getScrolledSearchHitsIterator(EsInvalidResponse("error")).toSeq shouldEqual Seq(EsInvalidResponse("error"))
  }

  behavior of "#getScrolledSearchIterator"
  it should "return an iterator of results with errors" in {
    val esClient = new EsClientWithMockedEs()
    esClient.getScrolledSearchIterator(EsError("error", 1)).toSeq shouldEqual Seq(EsError("error", 1))
    esClient.getScrolledSearchIterator(EsInvalidResponse("error")).toSeq shouldEqual Seq(EsInvalidResponse("error"))
  }

  behavior of "#buildScrolledSearchAction"
  it should "create a jest scrolled search action" in {
    val esClient = new EsClient(mock[JestClient])
    val query = "{}"
    val index = "index"
    val typeName = "type"

    def verify(search: Search, typeName: String) : Unit = {
      JsonUtils.equals(search.getData(new Gson()).toString(), query) shouldEqual true
      search.getIndex shouldEqual index
      search.getType shouldEqual typeName
      search.getParameter(Parameters.SCROLL).contains(EsNames.EsScanAndScrollTime) shouldEqual true
    }

    val search = esClient.buildScrolledSearchAction(query, index)
    verify(search, "")

    val searchWithTypeName = esClient.buildScrolledSearchAction(query, index, typeName)
    verify(searchWithTypeName, typeName)

    val searchWithParams = esClient.buildScrolledSearchAction(query, index, params = Map("fields" -> Seq("1$2")))
    searchWithParams.getParameter("fields").contains(Seq("1$2")) shouldEqual true
  }
}
