package com.workday.esclient.unit

import com.google.gson.{Gson, JsonObject}
import com.workday.esclient._
import com.workday.esclient.unit.EsClientSpec.EsClientWithMockedEs
import io.searchbox.client.JestClient
import io.searchbox.core.{Bulk, DeleteByQuery}
import io.searchbox.indices.{Analyze, Flush}
import org.mockito.Matchers.any
import org.mockito.Mockito._

class EsIndexingDocsSpec extends EsClientSpec {

  val TEST_INDEX_NAME = "index"
  val TEST_TYPE = "test_type"
  val TEST_ID = "test_id_1"
  val TEST_DOC = "{}"

  behavior of "#index"
  it should "index a doc" in {
    val response = UpdateResponse_1_7("", "", "", 1, false)
    val createIndexResponse = Acknowledgement_1_7(false)
    val esClient = new EsClientWithMockedEs()
      .whenCreateIndex(JsonUtils.toJson(createIndexResponse))
      .whenUpdate(JsonUtils.toJson(response))

    esClient.index(TEST_INDEX_NAME, TEST_TYPE, TEST_ID, TEST_DOC).get shouldEqual response
  }

  it should "fail to index a doc when provided with invalid arguments" in {
    val esClient = new EsClientWithMockedEs()
    val result = esClient.index("", "", "", "")
    result shouldBe an[EsInvalidResponse]
    a[NoSuchElementException] should be thrownBy {
      result.get
    }
  }

  behavior of "#getSleepTimeForBackpressure"
  it should "return sleep time" in {
    val esClient = new EsClient(mock[JestClient])

    esClient.getSleepTimeForBackpressure shouldBe 3000
  }

  behavior of "#bulkWithRetry"
  it should "not retry if bulk succeeds" in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val goodItem = BulkUpdateItemResponse_1_7(index = "test", typeName = "type", id = "id", version = 1, status = 200, error = None)
    val goodResult = EsResponse(BulkResponse(errors = false, items = Seq(goodItem, goodItem)))
    doReturn(goodResult).when(esClient).bulk(any[Seq[UpdateDocAction]])
    doReturn(10).when(esClient).getSleepTimeForBackpressure // We don't want our tests to actually wait for long

    val updateAction = UpdateDocAction("", "", "", "{}")
    val actions = Seq(updateAction, updateAction)
    esClient.bulkWithRetry(actions)
    verify(esClient, times(1)).bulk(any[Seq[UpdateDocAction]])
  }

  it should "handle EsInvalidResponse" in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val invalidResponse = EsInvalidResponse(msg = "Oops")
    doReturn(invalidResponse).when(esClient).bulk(any[Seq[UpdateDocAction]])

    val updateAction = UpdateDocAction("", "", "", "{}")
    val actions = Seq(updateAction, updateAction)
    esClient.bulkWithRetry(actions) shouldBe an[EsInvalidResponse]
    verify(esClient, times(1)).bulk(any[Seq[UpdateDocAction]])
  }

  it should "handle EsInvalidResponse on second try" in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val badItem = BulkUpdateItemResponse_1_7(index = "test", typeName = "type", id = "0", version = 1, status = 429, error = Some("Backpressure"))
    val badResult = EsResponse(BulkResponse(errors = false, items = Seq(badItem, badItem)))
    val invalidResponse = EsInvalidResponse(msg = "Oops")
    doReturn(badResult).doReturn(invalidResponse).when(esClient).bulk(any[Seq[UpdateDocAction]])
    doReturn(10).when(esClient).getSleepTimeForBackpressure // We don't want our tests to actually wait for long

    val updateAction = UpdateDocAction("", "", "", "{}")
    val actions = Seq(updateAction, updateAction)
    esClient.bulkWithRetry(actions) shouldBe badResult
    verify(esClient, times(2)).bulk(any[Seq[UpdateDocAction]])
  }

  it should "handle EsError_1_7" in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val errorResponse = EsError_1_7(error = "Something happened", status = 500)
    doReturn(errorResponse).when(esClient).bulk(any[Seq[UpdateDocAction]])

    val updateAction = UpdateDocAction("", "", "", "{}")
    val actions = Seq(updateAction, updateAction)
    esClient.bulkWithRetry(actions) shouldBe an[EsError_1_7]
    verify(esClient, times(1)).bulk(any[Seq[UpdateDocAction]])
  }

  it should "handle EsError" in {
    case class newEsError(error: String, status: Int) extends EsError {
      def get: Nothing = throw new NoSuchElementException(error + ", status " + status)
    }
    val esClient = spy(new EsClient(mock[JestClient]))
    val errorResponse = newEsError(error = "Something happened", status = 500)
    doReturn(errorResponse).when(esClient).bulk(any[Seq[UpdateDocAction]])

    val updateAction = UpdateDocAction("", "", "", "{}")
    val actions = Seq(updateAction, updateAction)
    esClient.bulkWithRetry(actions) shouldBe an[newEsError]
    verify(esClient, times(1)).bulk(any[Seq[UpdateDocAction]])
  }

  it should "handle depth past retries limit" in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val updateAction = UpdateDocAction("", "", "", "{}")
    val actions = Seq(updateAction, updateAction)
    val badItem = BulkUpdateItemResponse_1_7(index = "test", typeName = "type", id = "0", version = 1, status = 429, error = Some("Backpressure"))
    val badResult = EsResponse(BulkResponse(errors = false, items = Seq(badItem, badItem)))
    doReturn(badResult).when(esClient).bulk(any[Seq[UpdateDocAction]])
    esClient.bulkWithRetry(actions, depth = 0) shouldBe badResult
  }

  behavior of "#bulk"
  it should "not interact with ElasticSearch when given an empty sequence of actions" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClient(mockJestClient)
    esClient.bulk(Nil).get shouldEqual new BulkResponse(false, Nil)
    verifyZeroInteractions(mockJestClient)
  }

  it should "not perform a multiget action on ElasticSearch when only performing delete actions" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClientWithMockedEs(mockJestClient).whenBulk("""{"errors": false, "items":[{ "delete": {"_index":"","_type":"","_id":"","_version":0,"status":0,"found":false}}]}""")
    val actions = Seq(DeleteAction("", "", ""))
    esClient.bulk(actions).get shouldEqual new BulkResponse(false, Seq(BulkDeleteItemResponse_1_7("", "", "", 0, 0, false)))
    verify(mockJestClient, times(1)).execute(any[Bulk])
    verifyNoMoreInteractions(mockJestClient)
  }

  it should "bulk index/delete docs" in {
    val bulkResponseJson = """{ "errors": "false",
                             |  "items":
                             |   [
                             |     { "update": {"_index":"","_type":"","_id":"","_version":0,"status":0} },
                             |     { "delete": {"_index":"","_type":"","_id":"","_version":0,"status":0,"found":false} }
                             |   ]
                             |}""".stripMargin

    val multiGetResponseJson = """{"docs":[{ "_index": "", "_type": "", "_id": "", "_version": 0, "found": true, "_source": { "a": "b" }}]}"""

    val esClient = new EsClientWithMockedEs()
      .whenBulk(bulkResponseJson)
      .whenMultiGet(multiGetResponseJson)
      .whenCreateIndex(JsonUtils.toJson(Acknowledgement_1_7(false)))

    val response = BulkResponse(false, Seq(BulkUpdateItemResponse_1_7("", "", "", 0, 0), BulkDeleteItemResponse_1_7("", "", "", 0, 0, false)))
    val actions = Seq(
      UpdateDocAction("", "", "", "{}"), // this action should be performed
      DeleteAction("", "", ""), // so should this one
      UpdateDocAction("", "", "", """{ "a": "b" }""") // this one shouldn't -- the doc { "a": "b" } is already in ES
    )
    esClient.bulk(actions).get shouldEqual response
  }

  it should "throw an error on unrecognized response" in {
    val esResponseJson = """{ "errors": "false",
                           |  "items":
                           |   [
                           |     { "unexpected-response": {"_index":"","_type":"","_id":"","_version":0,"status":0} }
                           |   ]
                           |}""".stripMargin

    val esClient = new EsClientWithMockedEs().whenBulk(esResponseJson)
    val actions = Seq(DeleteAction("", "", ""))
    val ex = the[IllegalArgumentException] thrownBy { esClient.bulk(actions) }
    ex.getMessage should include ("unexpected-response")
  }

  it should "bulk update with a script if provided with UpdateScriptActions" in {
    val bulkResponseJson = """{ "errors": "false",
                             |  "items":
                             |   [
                             |     { "update": {"_index":"","_type":"","_id":"","_version":0,"status":0} },
                             |     { "update": {"_index":"","_type":"","_id":"","_version":0,"status":0} }
                             |   ]
                             |}""".stripMargin

    val esClient = new EsClientWithMockedEs()
      .whenBulk(bulkResponseJson)
      .whenCreateIndex(JsonUtils.toJson(Acknowledgement_1_7(false)))

    val response = BulkResponse(false, Seq(BulkUpdateItemResponse_1_7("", "", "", 0, 0), BulkUpdateItemResponse_1_7("", "", "", 0, 0)))
    val actions = Seq(
      UpdateScriptAction("index", "type", "docID", "script1"),
      UpdateScriptAction("index", "type2", "docID2", "script2"))

    esClient.bulk(actions).get shouldEqual response
  }

  behavior of "#forceFlush"
  it should "execute a Flush action" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClient(mockJestClient)

    esClient.forceFlush("")
    verify(mockJestClient).execute(any(classOf[Flush]))
  }

  behavior of "#buildDeleteAction"
  it should "add type and id if provided" in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClient(mockJestClient)

    esClient.buildDeleteByQueryAction("foo", None).getData(new Gson()) shouldBe "{\"query\": {\"match_all\": {}}}"

    val customQuery = "{\"query\": {\"match\": {\"foo\": \"bar\"}}}"
    esClient.buildDeleteByQueryAction("foo", Some(customQuery)).getData(new Gson()) shouldBe customQuery
  }

  behavior of "#deleteDocs"
  it should "execute a Delete action" in {
    val mockJestClient = mock[JestClient]
    val deleteResponse = "{\"_indices\": { \"ind\": { \"_shards\": { \"total\": 3, \"successful\": 3, \"failed\": 0 } } }}"
    val esClient = new EsClientWithMockedEs(mockJestClient).whenDeleteByQuery(deleteResponse)

    esClient.deleteDocsByQuery("foo")
    verify(esClient.jest).execute(any(classOf[DeleteByQuery]))
  }

  behavior of "#analyze"
  it should "execute an Analyze action and pass on the response" in {
    val mockJestClient = mock[JestClient]
    val response = AnalyzeResponse(Nil)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.analyze("").get shouldEqual response
    verify(mockJestClient).execute(any(classOf[Analyze]))
  }

  behavior of "#analyzeWithIndexAnalyzer"
  it should "execute an Analyze With Index Analyzer action and pass on the response" in {
    val mockJestClient = mock[JestClient]
    val response = AnalyzeResponse(Nil)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.analyzeWithIndexAnalyzer("", "", EsClient.edgeNgramsAnalyzer).get shouldEqual response
    verify(mockJestClient).execute(any(classOf[Analyze]))
  }

  "#buildUpdateAction" should "create a jest update action" in {
    val esClient = new EsClient(mock[JestClient])
    val idx = esClient.buildUpdateAction("1", "2", "3", esClient.makeIndexDocumentUpdateScript("{ }", true, true))
    idx.getIndex shouldEqual "1"
    idx.getType shouldEqual "2"
    idx.getId shouldEqual "3"
    idx.getData(new Gson()) shouldEqual """{ "doc" : { }, "doc_as_upsert" : true, "detect_noop": true}"""
  }

  "#buildBulkAction" should "create a jest bulk update action" in {
    val esClient = new EsClient(mock[JestClient])
    val actions = Seq(esClient.buildUpdateAction("1", "2", "3", esClient.makeIndexDocumentUpdateScript("4", true, true)))
    val bulk = esClient.buildBulkAction(actions)
    val bulkActionsJson = """{"update":{"_id":"3","_index":"1","_type":"2"}}"""
    JsonUtils.equals(bulk.getData(new Gson()), bulkActionsJson) shouldEqual true
  }

  behavior of "#createBulkItemResponse"
  it should "throw an error if the bulk API gives an unexpected response" in {
    val esClient = new EsClientWithMockedEs()
    val badJsonItemResponse = new JsonObject
    badJsonItemResponse.addProperty("invalidprop", 3)
    an[IllegalArgumentException] should be thrownBy { esClient.createBulkItemResponse(badJsonItemResponse) }
  }

  behavior of "#BulkItemResponse"
  it should "show error for status code of => 400" in {
    val badBulkIndex = new BulkUpdateItemResponse_1_7("", "", "", 0, 401)
    badBulkIndex.hasHttpError shouldBe true

    val badBulkDelete = new BulkDeleteItemResponse_1_7("", "", "", 0, 400, false)
    badBulkDelete.hasHttpError shouldBe true

    val goodBulkIndex = new BulkUpdateItemResponse_1_7("", "", "", 0, 200)
    goodBulkIndex.hasHttpError shouldBe false
  }
}

class UpdateDocActionSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll   with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar  {
  behavior of "#UpdateDocAction"
  it should "handle equality in obvoius case" in {
    val ua1 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    val ua2 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    ua1 shouldEqual ua2
  }

  it should "handle equality when json is messed up" in {
    val ua1 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    val ua2 = new UpdateDocAction("index", "type", "id", "{\"bar\": 123, \"foo\": \"bar\"}")
    ua1 shouldEqual ua2
  }

  it should "handle in-equality when index names differ" in {
    val ua1 = new UpdateDocAction("index1", "type", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    val ua2 = new UpdateDocAction("index2", "type", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    ua1 should not be ua2
  }

  it should "handle in-equality when SIDs differ" in {
    val ua1 = new UpdateDocAction("index", "type1", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    val ua2 = new UpdateDocAction("index", "type2", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    ua1 should not be ua2
  }

  it should "handle in-equality when IDs differ" in {
    val ua1 = new UpdateDocAction("index", "type", "id1", "{\"foo\": \"bar\", \"bar\": 123}")
    val ua2 = new UpdateDocAction("index", "type", "id2", "{\"foo\": \"bar\", \"bar\": 123}")
    ua1 should not be ua2
  }

  it should "handle in-equality when JSON data differ" in {
    val ua1 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    val ua2 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 456}")
    ua1 should not be ua2
  }

  it should "handle in-equality when JSON data is a superset" in {
    val ua1 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    val ua2 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123, \"test\": 12}")
    ua1 should not be ua2
  }

  it should "handle in-equality when JSON data is a subset" in {
    val ua1 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123, \"test\": 12}")
    val ua2 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123}")
    ua1 should not be ua2
  }

  it should "handle in-equality when data types differ" in {
    val ua1 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123, \"test\": 12}")
    val da2 = new DeleteAction("index", "type", "id")
    ua1 should not be da2
  }

  it should "compute hash" in {
    val ua1 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123, \"test\": 12}")
    val ua2 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"test\": 12, \"bar\": 123}")
    ua1.hashCode() shouldEqual ua2.hashCode()
  }
}

class DeleteActionSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll   with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar  {
  behavior of "#DeleteAction"
  it should "handle equality in obvoius case" in {
    val da1 = new DeleteAction("index", "type", "id")
    val da2 = new DeleteAction("index", "type", "id")
    da1 shouldEqual da2
  }

  it should "handle in-equality when index names differ" in {
    val da1 = new DeleteAction("index1", "type", "id")
    val da2 = new DeleteAction("index2", "type", "id")
    da1 should not be da2
  }

  it should "handle in-equality when SIDs differ" in {
    val da1 = new DeleteAction("index", "type1", "id")
    val da2 = new DeleteAction("index", "type2", "id")
    da1 should not be da2
  }

  it should "handle in-equality when IDs differ" in {
    val da1 = new DeleteAction("index", "type", "id1")
    val da2 = new DeleteAction("index", "type", "id2")
    da1 should not be da2
  }

  it should "handle in-equality when data types differ" in {
    val da1 = new DeleteAction("index", "type", "id")
    val ua2 = new UpdateDocAction("index", "type", "id", "{\"foo\": \"bar\", \"bar\": 123, \"test\": 12}")
    da1 should not be ua2
  }

  it should "compute hash" in {
    val da1 = new DeleteAction("index", "type", "id")
    val da2 = new DeleteAction("index", "type", "id")
    da1.hashCode() shouldEqual da2.hashCode()
  }
}
