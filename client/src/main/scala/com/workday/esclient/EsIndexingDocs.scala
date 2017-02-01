package com.workday.esclient

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Objects
import com.google.gson.JsonElement
import com.workday.esclient.actions.ErrorProcessingBulkBuilder
import io.searchbox.action.BulkableAction
import io.searchbox.client.JestResult
import io.searchbox.core._
import io.searchbox.indices.{Analyze, Flush}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaIteratorConverter}

/**
  * Elasticsearch Index Metadata APIs
  */
trait EsIndexingDocs extends JestUtils {
  val ESBackPressureErrorCode = 429
  val SleepTimeToHandleBackPressureMs = 3000
  val RetriesLimit = 5

  def getSleepTimeForBackpressure: Int = SleepTimeToHandleBackPressureMs

  val logger = LoggerFactory.getLogger(this.getClass)

  def index(index: String, typeName: String, id: String, doc: String): EsResult[UpdateResponse] = {
    if (index.isEmpty || typeName.isEmpty || id.isEmpty || doc.isEmpty) {
      EsInvalidResponse("Invalid arguments")
    } else {
      val updateAction = buildUpdateAction(index, typeName, id, makeIndexDocumentUpdateScript(doc, true, true))
      val jestResult = jest.execute(updateAction)

      toEsResult[UpdateResponse](jestResult)
    }
  }

  def bulkWithRetry(actions: Seq[Action], depth: Int = 1): EsResult[BulkResponse] = {
    /**
      * 1. Build a numbered list of all actions.
      * 2. call bulk get the results.
      * 3. Build a numbered list of response.
      *   1. Filter the numbered list for 429 response code.
      *   2. Extract the actions for the 429 indexed responses.
      *   3. Wait for a few seconds.
      *   4. Recursively call bulkRetry.
      *   5. Once the call comes back. Get the responses and add them to the existing response.
      *   6. Return response.
      */
    val bulkResponse = bulk(actions)
    val currentActions = actions.zipWithIndex.map{t => (t._2, t._1)}

    bulkResponse match {
      case EsResponse(originalBulkResponseValue) =>
        val bulkResponseIndexedMap = originalBulkResponseValue.items.zipWithIndex.map{t => (t._2, t._1)}.toMap
        val rejectedBulkResponseByIndex = bulkResponseIndexedMap.filter(kv => kv._2.status == ESBackPressureErrorCode)
        val backpressureActions = currentActions.filter(indexActionTuple => rejectedBulkResponseByIndex.contains(indexActionTuple._1))

        if (backpressureActions.nonEmpty) {
          if (depth >= RetriesLimit) {
            logger.info(s"Got ${backpressureActions.size} backpressure responses for ${actions.size} actions.  Retry limit exceeded.")
            EsResponse(originalBulkResponseValue)
          } else {
            logger.info(s"Got ${backpressureActions.size} backpressure responses for ${actions.size} actions.  Retrying failed actions.")
            Thread.sleep(getSleepTimeForBackpressure)

            val retryResponse = bulkWithRetry(backpressureActions.map(_._2), depth + 1)
            retryResponse match {
              case EsResponse(retryResponseValue) => {
                val retriedRequestResponses = rejectedBulkResponseByIndex.keys.toSeq.sortWith(_ <= _).zip(retryResponseValue.items).toMap
                val nonQueuePressureError = originalBulkResponseValue.items.exists(response => response.hasError && response.status != ESBackPressureErrorCode)

                EsResponse(BulkResponse(nonQueuePressureError & retryResponseValue.errors,
                  (bulkResponseIndexedMap ++ retriedRequestResponses).toSeq.sortWith(_._1 < _._1).map(_._2)))
              }
              case _ => EsResponse(originalBulkResponseValue)
            }
          }
        }
        else
          EsResponse(originalBulkResponseValue)
      case e: EsInvalidResponse => e
      case e: EsError => e
    }
  }

  // scalastyle:off cyclomatic.complexity
  def bulk(actions: Seq[Action]): EsResult[BulkResponse] = {
    lazy val EmptyResponse = new EsResponse[BulkResponse](new BulkResponse(false, Nil))

    if(actions.isEmpty) {
      EmptyResponse
    } else {
      val docUpdateActions = actions collect { case a: UpdateDocAction => a}
      val deleteActions = actions collect { case a: DeleteAction => a}
      val scriptUpdateActions = actions collect { case a: UpdateScriptAction => a}

      val jestIndexActions = docUpdateActions.map { case action : UpdateDocAction =>
        // We were given the document, so just update it
        buildUpdateAction(action.index, action.typeName, action.id, makeIndexDocumentUpdateScript(action.doc, true , true))
      }
      val jestDeleteActions = deleteActions map { case DeleteAction(index, typeName, id) =>
        new Delete.Builder(id).index(index).`type`(typeName).build
      }
      val jestScriptActions = scriptUpdateActions.map { case action : UpdateScriptAction =>
        // We were given a specific script to run, so do an update with that script as-is
        buildUpdateAction(action.index, action.typeName, action.id, action.script)
      }

      val jestActions = jestIndexActions ++ jestDeleteActions ++ jestScriptActions
      val action = buildBulkAction(jestActions)
      val jestResult = jest.execute(action)
      handleJestResult(jestResult) { successfulJestResult: JestResult =>
        val items = successfulJestResult.getJsonObject.get("items").getAsJsonArray.iterator().asScala
        val itemResponses = items.map(createBulkItemResponse).toSeq
        BulkResponse(successfulJestResult.getJsonObject.get("errors").getAsBoolean, itemResponses)
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  def forceFlush(index: String): Unit = {
    jest.execute(new Flush.Builder().addIndex(index).setParameter("force", "").build())
  }

  def analyze(source: String): EsResult[AnalyzeResponse] = {
    val jestResult = jest.execute(new Analyze.Builder().source(source).build())
    toEsResult[AnalyzeResponse](jestResult)
  }

  // Will use whatever analyzer is specified, defaulting to "standard" if none
  def analyzeWithIndexAnalyzer(source: String, index: String, analyzer: String = EsNames.standardAnalyzerName): EsResult[AnalyzeResponse] = {
    val jestResult = jest.execute(new Analyze.Builder().source(source).index(index).analyzer(analyzer).build())
    toEsResult[AnalyzeResponse](jestResult)
  }

  def deleteDocsByQuery(index: String, query: Option[String] = None): EsResult[DeleteByQueryResponse] = {
    // Default query is "match all"
    val jestResult = jest.execute(buildDeleteByQueryAction(index, query = query))
    toEsResult[DeleteByQueryResponse](jestResult)
  }

  /**
    * Build an update action for a document with the provided update payload (as defined by
    * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html)
    */
  @VisibleForTesting
  private[esclient] def buildUpdateAction(index: String, typeName: String, id: String, updatePayload: String): Update = {
    new Update.Builder(updatePayload).index(index).`type`(typeName).id(id).build
  }

  @VisibleForTesting
  private[esclient] def buildBulkAction(actions: Seq[BulkableAction[DocumentResult]]): Bulk = {
    new ErrorProcessingBulkBuilder().addAction(actions.asJavaCollection).build
  }

  @VisibleForTesting
  private[esclient] def createBulkItemResponse(itemJson: JsonElement): BulkItemResponse = {
    val itemObj = itemJson.getAsJsonObject

    val response = if (itemObj.has("update")) {
      JsonUtils.fromJson[BulkUpdateItemResponse](itemObj.get("update").toString)
    } else if (itemObj.has("delete")) {
      JsonUtils.fromJson[BulkDeleteItemResponse](itemObj.get("delete").toString)
    } else {
      throw new IllegalArgumentException(itemObj.toString)
    }

    response
  }

  @VisibleForTesting
  private[esclient] def buildDeleteByQueryAction(indexName: String, query: Option[String] = None): DeleteByQuery = {
    val allQuery = "{\"query\": {\"match_all\": {}}}"
    new DeleteByQuery.Builder(query.getOrElse(allQuery)).addIndex(indexName).build()
  }

  /**
    * Given a document, returns the update script that will update the document (or add it to the index it if necessary)
    */
  @VisibleForTesting
  private[esclient] def makeIndexDocumentUpdateScript(document: String, upsert: Boolean = true, noop: Boolean = true): String = {
    "{ \"doc\" : " + document + ", \"doc_as_upsert\" : " + upsert.toString + ", \"detect_noop\": " + noop.toString + "}"
  }
}

case class BulkResponse(
  errors: Boolean,
  items: Seq[BulkItemResponse]
)

sealed trait BulkItemResponse {
  def index: String
  def typeName: String
  def id: String
  def version: Int
  def status: Int
  def error: Option[String]
  def hasError: Boolean = status >= 400
}
case class BulkUpdateItemResponse(
  @JsonProperty(EsClient._INDEX) index: String,
  @JsonProperty(EsClient._TYPE) typeName: String,
  @JsonProperty(EsClient._ID) id: String,
  @JsonProperty(EsClient._VERSION) version: Int,
  status: Int,
  error: Option[String] = None
) extends BulkItemResponse

case class BulkDeleteItemResponse(
  @JsonProperty(EsClient._INDEX) index: String,
  @JsonProperty(EsClient._TYPE) typeName: String,
  @JsonProperty(EsClient._ID) id: String,
  @JsonProperty(EsClient._VERSION) version: Int,
  status: Int,
  found: Boolean,
  error: Option[String] = None
) extends BulkItemResponse

case class AnalyzeResponse(
  tokens: Seq[Token]
)

case class DeleteIndexStats(
  @JsonProperty("total") total: Int,
  @JsonProperty("successful") successful: Int,
  @JsonProperty("failed") failed: Int
)

case class DeleteIndexData(
  @JsonProperty("_shards") shards: DeleteIndexStats
)

case class DeleteByQueryResponse(
  @JsonProperty("_indices") indices: Map[String, DeleteIndexData]
)

case class Token(
  token: String,
  startOffset: Int,
  endOffset: Int,
  `type`: String,
  position: Int
)

/**
  * Note that even if we give these names like "_index" Jackson still won't serialize it automatically
  */
case class UpdateResponse(
  @JsonProperty(EsClient._INDEX) index: String,
  @JsonProperty(EsClient._TYPE) typeName: String,
  @JsonProperty(EsClient._ID) id: String,
  @JsonProperty(EsClient._VERSION) version: Int,
  created: Boolean
)

// Update a document by passing the changed fields
case class UpdateDocAction(index: String, typeName: String, id: String, doc: String) extends Action {
  override def equals(that: Any): Boolean = {
    // We want equality to compare parsed JSON instead of `doc` string
    that match {
      case that: UpdateDocAction =>
        this.index == that.index && this.typeName == that.typeName && this.id == that.id && JsonUtils.equals(this.doc, that.doc)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    // TODO: How can we hash arbitrary JSON properly, so that `{"foo": 1, "bar": 2}` and `{"bar": 2, "foo": 1}` give the same hash?
    Objects.hashCode(this.index, this.typeName, this.id, JsonUtils.fromJson[Map[String, Any]](this.doc))
  }
}

sealed trait Action {
  def index: String
  def typeName: String
  def id: String
}

case class DeleteAction(index: String, typeName: String, id: String) extends Action

case class UpdateScriptAction(index: String, typeName: String, id: String, script: String) extends Action // Update a document using a script
