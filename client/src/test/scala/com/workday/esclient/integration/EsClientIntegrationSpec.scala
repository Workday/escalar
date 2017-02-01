package com.workday.esclient.integration

import java.net.SocketException

import com.workday.esclient._
import io.searchbox.indices.CreateIndex

import scala.collection.JavaConversions.mapAsJavaMap
import scala.util.Random

class EsClientIntegrationSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll
  with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar  {

  private[this] val esUrl = "http://localhost:18103"
  private[this] val jestClient = EsClient.createJestClient(esUrl)
  private[this] val esClient = new EsClient(jestClient)

  private[this] val index = ("esclientspec@" ++ new Random().alphanumeric.take(10) + EsNames.NAME_DELIMITER + EsNames.DOCS_SUFFIX).toLowerCase
  private[this] val testIndex = "esclientspec@test_index@docs"
  private[this] val docType = "1$123"
  private[this] val id = "testid"
  private[this] val dataIndexName = "data"
  private[this] val usersIndexName = "users"
  private[this] val mappingRetryMillis = 1000
  private[this] val mappingRetriesLimit = 10

  val doc = """{ "user": "tkim", "creation_time_ms": 123 }"""
  val docDontExceedRawLength = s"""{ "resume": "${"a" * EsNames.OtherMappingRawFieldIgnoreAboveLength}" }"""
  val docExceedRawLength = s"""{ "resume": "${"a" * (EsNames.OtherMappingRawFieldIgnoreAboveLength + 1)}" }"""
  val nonEncryptedIndexSettings = Some(Map("settings" -> Map("index.store.type" -> "default", "index.translog.type" -> "org.elasticsearch.index.translog.fs.FsTranslog")))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    esClient.createIndex(testIndex, nonEncryptedIndexSettings)
    esClient.createIndex(index, nonEncryptedIndexSettings)
    esClient.createIndex(dataIndexName, nonEncryptedIndexSettings)
    esClient.createIndex(usersIndexName, nonEncryptedIndexSettings)
  }

  override protected def afterAll(): Unit = {
    esClient.deleteIndex(testIndex)
    esClient.deleteIndex(index)
    esClient.deleteIndex(dataIndexName)
    esClient.deleteIndex(usersIndexName)
    esClient.clearCacheKeys(Nil)
    super.afterAll()
  }


  private[this] def getMappingsForType(index: String, typeName: String, depth: Int = 0): IndexMappings = {
    val mappingsResult: IndexMappings = esClient.getMappingsForIndex(index).get
    mappingsResult.mappings.contains(typeName) match {
      case true => mappingsResult
      case false =>
        depth >= mappingRetriesLimit match {
          case true => {
            fail(s"Mappings do not contain ${typeName} after ${depth} iterations")
          }
          case false => {
            Thread.sleep(mappingRetryMillis)
            getMappingsForType(index, typeName, depth + 1)
          }
        }
    }
  }

  behavior of "#createJestClient"
  it should "return a working client" in {
    noException should be thrownBy jestClient.execute(new CreateIndex.Builder(testIndex).build())
  }

  it should "fail when the url is bad" in {
    val badUrl = "http://localhost:1234"
    val badClient = EsClient.createJestClient(badUrl)
    an[SocketException] should be thrownBy {
      badClient.execute(new CreateIndex.Builder(testIndex).build())
    }
  }

  behavior of "#index"
  it should "index a doc, get it back, and count the index properly" in {
    val result = esClient.index(index, docType, id, doc).get
    result.index shouldEqual index
    result.typeName shouldEqual docType
    result.id shouldEqual id

    val getResult = esClient.get(index, id).get
    getResult.index shouldEqual index
    getResult.typeName shouldEqual docType
    getResult.id shouldEqual id
    getResult.version shouldEqual 1
    getResult.found shouldEqual true
    JsonUtils.equals(getResult.source.get, doc) shouldEqual true

    esClient.forceFlush(index)
    val countResult = esClient.count(index).get
    countResult shouldEqual 1

    val searchResult = esClient.search(index, docType, """{ "query": { "term": { "user": "tkim" } } }""").get
    searchResult.hits.total shouldEqual 1
    JsonUtils.equals(searchResult.hits.hits(0).source, doc) shouldEqual true

    val mappingsResult = getMappingsForType(index, docType)
    val props = mappingsResult.mappings(docType)
    props.properties("user").`type` shouldEqual "string"
  }

  it should "not store raw mapping of strings longer than EsNames.OtherMappingRawFieldIgnoreAboveLength characters" in {
    esClient.index(index, docType, id, docExceedRawLength).get
    esClient.forceFlush(index)

    val searchResult = esClient.search(index, docType, s"""{ "query": { "match": { "resume.raw": "${"a" * (EsNames.OtherMappingRawFieldIgnoreAboveLength + 1)}" } } }""").get
    searchResult.hits.total shouldEqual 0
  }

  it should "store raw mapping of strings equal to or less than EsNames.OtherMappingRawFieldIgnoreAboveLength characters" in {
    esClient.index(index, docType, id, docDontExceedRawLength).get
    esClient.forceFlush(index)

    val searchResult = esClient.search(index, docType, s"""{ "query": { "match": { "resume.raw": "${"a" * EsNames.OtherMappingRawFieldIgnoreAboveLength}" } } }""").get
    searchResult.hits.total shouldEqual 1
  }

  it should "get a bad request when indexing a document without specifying a type" in {
    val result = esClient.index(index, "", "123", "{}")
    result shouldBe an[EsInvalidResponse]
    a[NoSuchElementException] should be thrownBy {
      result.get
    }
  }

  behavior of "#get"
  it should "get a not found error when getting a document from a non-existent index" in {
    val result = esClient.get("non_existent_index", "123")
    result shouldBe an[EsError]
    result.asInstanceOf[EsError].status shouldEqual 404

    a[NoSuchElementException] should be thrownBy {
      result.get
    }
  }

  it should "return found=false and source=None when getting a document that doesn't exist" in {
    val result = esClient.get(index, "non-existent-id").get
    result.found shouldEqual false
    result.source shouldEqual None
  }

  behavior of "#multiGet"
  it should "get a document" in {
    val result = esClient.multiGet(index, docType, Seq(id)).get
    result.docs.length shouldEqual 1
    val getResponse = result.docs.head
    getResponse.index shouldEqual index
    getResponse.typeName shouldEqual docType
    getResponse.id shouldEqual id
    getResponse.found shouldEqual true
  }

  it should "get only requested fields in a document" in {
    esClient.index(index, docType, id, doc).get
    esClient.forceFlush(index)
    val result = esClient.multiGetSourceField(index, docType, Seq(id), "creation_time_ms").get
    result.docs.length shouldEqual 1
    val getResponse = result.docs.head
    getResponse.index shouldEqual index
    getResponse.typeName shouldEqual docType
    getResponse.id shouldEqual id
    getResponse.found shouldEqual true
    JsonUtils.toJson(getResponse.sourceJson) shouldEqual """{"creation_time_ms":123}"""
  }

  behavior of "#bulk"
  it should "bulk actions together" in {
    val result = esClient.bulk(Seq(UpdateDocAction(index, docType, "111", "{}"), DeleteAction(index, docType, "222"))).get
    result shouldEqual BulkResponse(false,
      Seq(
        BulkUpdateItemResponse(index, docType, "111", 1, 201),
        BulkDeleteItemResponse(index, docType, "222", 1, 404, false)
        ))
  }

  it should "fail to bulk index items in an uncreated index" in {
    val newIndex = index + "new"
    a[IllegalArgumentException] should be thrownBy {
      esClient.bulk(Seq(UpdateDocAction(newIndex, docType, "111", "{}"), DeleteAction(newIndex, docType, "222"))).get
    }
  }

  it should "always index for bulk actions" in {
    val id = "bulkindexdontskip2"
    esClient.index(index, docType, id, "{}")
    val actions = Seq(UpdateDocAction(index, docType, id, "{}"), UpdateDocAction(index, docType, id, """{"a":"b"}"""))
    val result = esClient.bulk(actions).get
    result shouldEqual BulkResponse(false,
      Seq(
        BulkUpdateItemResponse(index, docType, id, 1, 200),
        BulkUpdateItemResponse(index, docType, id, 2, 200)
      ))
  }

  it should "delete a doc" in {
    val id = "deleteid1"
    esClient.index(index, docType, id, """{ "stuff": "stuff", "morestuff": "morestuff" }""")
    esClient.forceFlush(index)
    esClient.get(index, id).get.found shouldEqual true

    val deleteResponse = esClient.bulk(Seq(DeleteAction(index, docType, id))).get.items.head.asInstanceOf[BulkDeleteItemResponse]
    deleteResponse.status shouldEqual 200
    deleteResponse.found shouldEqual true
    deleteResponse.id shouldEqual id

    esClient.forceFlush(index)

    esClient.get(index, id).get.found shouldEqual false
  }

  behavior of "#health"
  it should "show health" in {
    val result = esClient.clusterHealth.get
    result.status should (equal ("yellow") or equal ("green"))
  }

  behavior of "#analyze"
  it should "analyze a query string using the standard analyzer by default" in {
    esClient.analyze("{}").get shouldEqual AnalyzeResponse(Nil)

    val searchString = "{UI Test - Buttons (View)}"
    val response = EsResponse(AnalyzeResponse(
      Seq(Token("ui", 1, 3, "<ALPHANUM>", 1),
        Token("test", 4, 8, "<ALPHANUM>", 2),
        Token("buttons", 11, 18, "<ALPHANUM>", 3),
        Token("view", 20, 24, "<ALPHANUM>", 4))))
    esClient.analyze(searchString) shouldEqual response
  }

  behavior of "#analyzeWithIndexAnalyzer"
  it should "analyze a query string using the standard analyzer by default" in {
    esClient.analyzeWithIndexAnalyzer("{}", index).get shouldEqual AnalyzeResponse(Nil)

    val searchString = "{UI Test}"
    val response = EsResponse(AnalyzeResponse(
      Seq(Token("ui", 1, 3, "<ALPHANUM>", 1),
        Token("test", 4, 8, "<ALPHANUM>", 2))))
    esClient.analyzeWithIndexAnalyzer(searchString, index) shouldEqual response
  }
  it should "analyze a string using the edge ngrams analyzer when specified" in {
    esClient.analyzeWithIndexAnalyzer("{}", index, EsClient.edgeNgramsAnalyzer).get shouldEqual AnalyzeResponse(Nil)

    val searchString = "{UI Test}"
    val response = EsResponse(AnalyzeResponse(
      Seq(Token("u", 1, 3, "word", 1),
        Token("ui", 1, 3, "word", 1),
        Token("t", 4, 8, "word", 2),
        Token("te", 4, 8, "word", 2),
        Token("tes", 4, 8, "word", 2),
        Token("test", 4, 8, "word", 2))))
    esClient.analyzeWithIndexAnalyzer(searchString, index, EsClient.edgeNgramsAnalyzer) shouldEqual response
  }

  behavior of "#createIndex"
  it should "get an error when creating an index that already exists" in {
    val badIndexCall = esClient.createIndex(index)
    badIndexCall shouldBe an[EsError]
    badIndexCall.asInstanceOf[EsError].status shouldEqual 400

    a[NoSuchElementException] should be thrownBy {
      badIndexCall.get
    }
  }

  behavior of "#getMappingsForIndex"
  it should "return the mappings for an index with the correct field properties" in {
    esClient.index(index, docType, id, doc).get
    esClient.forceFlush(index)
    val fieldProperties = esClient.getMappingsForIndex(index).get.mappings(docType).properties
    fieldProperties("user") shouldEqual IndexFieldProperties("string")
  }

  behavior of "#clearCache"
  it should "return stale results when the cache is uncleared, then return correct results after clearing the cache" in {
    // Set up data
    esClient.index(dataIndexName, "dataType", "1", """{ "labels": [ "privileged" ] }""")
    esClient.index(usersIndexName, "userType", "1", """{ "labels": [ "privileged" ] }""")

    esClient.forceFlush(dataIndexName)
    esClient.forceFlush(usersIndexName)

    val cacheKey = "my_cache_key"
    val termsLookupCacheKey = Seq(usersIndexName, "userType", "1", "labels").mkString("/")

    // Clear the cache from any previous runs
    esClient.clearCacheKeys(Seq(cacheKey, termsLookupCacheKey))

    // Query using terms lookup.  First query establishes the cache.
    val query =
      s"""{ "query":
        |  { "filtered":
        |    { "filter":
        |      { "terms":
        |        { "labels":
        |          { "index": "${usersIndexName}",
        |            "type": "userType",
        |            "id": "1",
        |            "path": "labels"
        |          },
        |          "_cache_key": "${cacheKey}"
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
    val results = esClient.search(dataIndexName, query)

    results.get.hits.total shouldEqual 1

    // Change the data
    esClient.index(usersIndexName, "userType", "1", """ { "labels": [ ] } """)
    esClient.forceFlush(usersIndexName)

    // Query again, we should get stale results
    esClient.search(dataIndexName, query).get.hits.total shouldEqual 1

    // Clear the cache
    esClient.clearCacheKeys(Seq(cacheKey, termsLookupCacheKey))

    // Query again, we should get correct results
    esClient.search(dataIndexName, query).get.hits.total shouldEqual 0
  }

  behavior of "#updateClusterSettings"
  it should "update cluster settings" in {
    val firstUpdateResult = esClient.updateClusterSettings(mapAsJavaMap(Map("cluster.routing.allocation.enable" -> "primaries")), mapAsJavaMap(Map[String, String]()))
    firstUpdateResult.get.transient shouldEqual Map("cluster" -> Map("routing" -> Map("allocation" -> Map("enable" -> "primaries"))))
    esClient.clusterSettings.get.transient shouldEqual Map("cluster" -> Map("routing" -> Map("allocation" -> Map("enable" -> "primaries"))))
    // Update and check again for change in settings, since previous check may have matched persisted settings
    val secondUpdateResult = esClient.updateClusterSettings(mapAsJavaMap(Map("cluster.routing.allocation.enable" -> "all")), mapAsJavaMap(Map[String, String]()))
    secondUpdateResult.get.transient shouldEqual Map("cluster" -> Map("routing" -> Map("allocation" -> Map("enable" -> "all"))))
    esClient.clusterSettings.get.transient shouldEqual Map("cluster" -> Map("routing" -> Map("allocation" -> Map("enable" -> "all"))))
  }

}
