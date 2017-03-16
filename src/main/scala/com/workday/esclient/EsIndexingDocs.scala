/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

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
import org.apache.log4j.Logger

import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaIteratorConverter}

/**
  * Trait wrapping Elasticsearch Index Metadata APIs
  */
trait EsIndexingDocs extends JestUtils {
  val ESBackPressureErrorCode = 429
  val SleepTimeToHandleBackPressureMs = 3000
  val RetriesLimit = 5

  /**
    * Gets [[com.workday.esclient.EsIndexingDocs.SleepTimeToHandleBackPressureMs]]
    * @return Int [[com.workday.esclient.EsIndexingDocs.SleepTimeToHandleBackPressureMs]]
    */
  def getSleepTimeForBackpressure: Int = SleepTimeToHandleBackPressureMs

  val logger = Logger.getLogger(this.getClass)

  /**
    * Indexes a given doc to an Elasticsearch index under the given id.
    * @param index String ES index name.
    * @param typeName String ES type name.
    * @param id String id to index document under.
    * @param doc String doc to index.
    * @return EsResult of [[com.workday.esclient.GenericUpdateResponse]]
    */
  def index(index: String, typeName: String, id: String, doc: String): EsResult[GenericUpdateResponse] = {
    if (index.isEmpty || typeName.isEmpty || id.isEmpty || doc.isEmpty) {
      EsInvalidResponse("Invalid arguments")
    } else {
      val updateAction = buildUpdateAction(index, typeName, id, makeIndexDocumentUpdateScript(doc, true, true))
      val jestResult = jest.execute(updateAction)

      toEsResult[UpdateResponse](jestResult)
    }
  }

  /**
    * Attempts a bulk sequence of Elasticsearch actions and retries on failure up to a given depth of retries.
    * @param actions Sequence of action to attempt.
    * @param depth Int recursion depth limit. Defaults to 1. [[com.workday.esclient.EsIndexingDocs.RetriesLimit]] is set to 5.
    * @return EsResult of [[com.workday.esclient.BulkResponse]]
    */
  def bulkWithRetry(actions: Seq[Action], depth: Int = 1): EsResult[BulkResponse] = {
    /*
      * 1. Build a numbered list of all actions.
      * 2. call bulk get the results.
      * 3. Build a numbered list of response.
      *   a. Filter the numbered list for 429 response code.
      *   b. Extract the actions for the 429 indexed responses.
      *   c. Wait for a few seconds.
      *   d. Recursively call bulkRetry.
      *   e. Once the call comes back. Get the responses and add them to the existing response.
      *   f. Return response.
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
        else {
          EsResponse(originalBulkResponseValue)
        }
      case e: EsInvalidResponse => e
      case e: EsError => e
      case e: GenericEsError => handleEsResult(e)
    }
  }

  /**
    * Parses a given Elasticsearch action sequence and executes the actions.
    * @param actions Sequence of ES update, index, and delete actions to perform.
    * @return EsResult of [[com.workday.esclient.BulkResponse]]
    */
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

  /**
    * Flushes an Elasticsearch index.
    * @param index String ES index name.
    */
  def forceFlush(index: String): Unit = {
    jest.execute(new Flush.Builder().addIndex(index).setParameter("force", "").build())
  }

  /**
    * Submits the given source for analysis using an Elasticsearch Analyzer and returns the response.
    * @param source String source to analyze.
    * @return EsResult of [[com.workday.esclient.AnalyzeResponse]]
    */
  def analyze(source: String): EsResult[AnalyzeResponse] = {
    val jestResult = jest.execute(new Analyze.Builder().source(source).build())
    toEsResult[AnalyzeResponse](jestResult)
  }

  /**
    * Submits the given source for analysis using an Elasticsearch Analyzer, defautlting to the standard analyzer.
    * @param source String source to analyze.
    * @param index String ES index name.
    * @param analyzer String name of ES Analyzer to use.
    * @return EsResult of [[com.workday.esclient.AnalyzeResponse]]
    */
  // Will use whatever analyzer is specified, defaulting to "standard" if none
  def analyzeWithIndexAnalyzer(source: String, index: String, analyzer: String = EsNames.standardAnalyzerName): EsResult[AnalyzeResponse] = {
    val jestResult = jest.execute(new Analyze.Builder().source(source).index(index).analyzer(analyzer).build())
    toEsResult[AnalyzeResponse](jestResult)
  }

  /**
    * Deletes Elasticsearch documents that match a given query string.
    * @param index String ES index name.
    * @param query Optional string to query ES. Defaults to None.
    * @return EsResult of [[com.workday.esclient.DeleteByQueryResponse]]
    */
  def deleteDocsByQuery(index: String, query: Option[String] = None): EsResult[DeleteByQueryResponse] = {
    // Default query is "match all"
    val jestResult = jest.execute(buildDeleteByQueryAction(index, query = query))
    toEsResult[DeleteByQueryResponse](jestResult)
  }

  /**
    * Override-able method to catch new ES error case classes and return the contents.
    * @param e EsResult[Nothing] to represent child error case classes.
    * @return EsResult[Nothing] the error case class.
    */
  protected def handleEsResult(e: GenericEsError): GenericEsError = e

  /**
    * Builds an update action for a document with the provided update payload.
    * As defined by: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
    * @param index String ES index name.
    * @param id String id of document to update.
    * @param updatePayload String actual payload to update document with.
    * @return [[io.searchbox.core.Update]] object for Elasticsearch.
    */
  @VisibleForTesting
  private[esclient] def buildUpdateAction(index: String, typeName: String, id: String, updatePayload: String): Update = {
    new Update.Builder(updatePayload).index(index).`type`(typeName).id(id).build
  }

  /**
    * Builds a bulk action for Elasticsearch given a sequence of actions.
    * @param actions Sequence of [[io.searchbox.action.BulkableAction]].
    * @return [[io.searchbox.core.Bulk]] object for Elasticsearch.
    */
  @VisibleForTesting
  private[esclient] def buildBulkAction(actions: Seq[BulkableAction[DocumentResult]]): Bulk = {
    new ErrorProcessingBulkBuilder().addAction(actions.asJavaCollection).build
  }

  /**
    * Creates a bulk response given a JSON response from Elasticsearch.
    * @param itemJson [[com.google.gson.JsonElement]] from Elasticsearch.
    * @return [[com.workday.esclient.BulkItemResponse]].
    */
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

  /**
    * Builds a Delete By Query action for Elasticsearch.
    * Uses a match_all query is none is provided.
    * @param indexName String ES index name.
    * @param query Optional string for the ES query. Defaults to None.
    * @return [[io.searchbox.core.DeleteByQuery]] object for Elasticsearch.
    */
  @VisibleForTesting
  private[esclient] def buildDeleteByQueryAction(indexName: String, query: Option[String] = None): DeleteByQuery = {
    val allQuery = "{\"query\": {\"match_all\": {}}}"
    new DeleteByQuery.Builder(query.getOrElse(allQuery)).addIndex(indexName).build()
  }

  /**
    * Returns the update script that will update the given document or add it to the index it if necessary.
    * @param document String of the actual document.
    * @param upsert Boolean whether this is an upsert action. Defaults to true.
    * @param noop Boolean whether the action is a no-op action. Defaults to true.
    * @return String of JSON to use as the index document update script for ES.
    */
  @VisibleForTesting
  private[esclient] def makeIndexDocumentUpdateScript(document: String, upsert: Boolean = true, noop: Boolean = true): String = {
    "{ \"doc\" : " + document + ", \"doc_as_upsert\" : " + upsert.toString + ", \"detect_noop\": " + noop.toString + "}"
  }
}

/**
  * Case class for an Elasticsearch Bulk response.
  * @param errors Boolean whether any errors were reported.
  * @param items Sequence of [[com.workday.esclient.BulkItemResponse]] from ES.
  */
case class BulkResponse(
  errors: Boolean,
  items: Seq[BulkItemResponse]
)

/**
  * Trait wrapping a Bulk item response from Elasticsearch.
  * Currently is extensible for handling other types of Bulk responses not currently implemented here.
  */
trait BulkItemResponse {
  def index: String
  def typeName: String
  def id: String
  def version: Int
  def status: Int
  def error: Option[String]
  def hasError: Boolean = status >= 400
}

/**
  * Case class for a Bulk update response from Elasticsearch.
  * @param index String ES index name.
  * @param typeName String ES type name.
  * @param id String id of actual document.
  * @param version Int document version number.
  * @param status Int status code of response.
  * @param error Optional string for any errors. Defaults to None.
  */
case class BulkUpdateItemResponse(
  @JsonProperty(EsClient._INDEX) index: String,
  @JsonProperty(EsClient._TYPE) typeName: String,
  @JsonProperty(EsClient._ID) id: String,
  @JsonProperty(EsClient._VERSION) version: Int,
  status: Int,
  error: Option[String] = None
) extends BulkItemResponse

/**
  * Case class for a Bulk delete response from Elasticsearch.
  * @param index String ES index name.
  * @param typeName String ES type name.
  * @param id String id of actual document.
  * @param version Int document version number.
  * @param status Int status code of response.
  * @param error Optional string for any errors. Defaults to None.
  */
case class BulkDeleteItemResponse(
  @JsonProperty(EsClient._INDEX) index: String,
  @JsonProperty(EsClient._TYPE) typeName: String,
  @JsonProperty(EsClient._ID) id: String,
  @JsonProperty(EsClient._VERSION) version: Int,
  status: Int,
  found: Boolean,
  error: Option[String] = None
) extends BulkItemResponse

/**
  * Case class for an Analyze response from Elasticsearch.
  * @param tokens Sequence of [[com.workday.esclient.Token]] representing tokens from the original string.
  */
case class AnalyzeResponse(
  tokens: Seq[Token]
)

/**
  * Case class for delete index statistics.
  * @param total Int total attempted deleted.
  * @param successful Int number successfully deleted.
  * @param failed Int number not deleted.
  */
case class DeleteIndexStats(
  @JsonProperty("total") total: Int,
  @JsonProperty("successful") successful: Int,
  @JsonProperty("failed") failed: Int
)

/**
  * Case class for capturing shard deletion data.
  * @param shards [[com.workday.esclient.DeleteIndexStats]] for the shards deleted.
  */
case class DeleteIndexData(
  @JsonProperty("_shards") shards: DeleteIndexStats
)

/**
  * Case class for a Delete by Query response from Elasticsearch.
  * @param indices Map of indices and their shard deletion data.
  */
case class DeleteByQueryResponse(
  @JsonProperty("_indices") indices: Map[String, DeleteIndexData]
)

/**
  * Case class for a token return by the Elasticsearch Analyze API.
  * @param token String actual token returned.
  * @param startOffset Int start offset from original string.
  * @param endOffset Int end offset from original string.
  * @param `type` String type of token.
  * @param position Int position in the token sequence.
  */
case class Token(
  token: String,
  startOffset: Int,
  endOffset: Int,
  `type`: String,
  position: Int
)

/**
  * Generic trait for an Update response.
  */
trait GenericUpdateResponse {
  def index: String
  def typeName: String
  def id: String
  def version: Int
  def created: Boolean
}

/**
  * Case class for an Update response from Elasticsearch.
  * Note that even if we give these names like "_index" Jackson still won't serialize it automatically.
  * @param index String ES index name.
  * @param typeName String ES type name.
  * @param id String document id.
  * @param version Int document version number.
  * @param created Boolean whether document was created.
  */
case class UpdateResponse(
  @JsonProperty(EsClient._INDEX) index: String,
  @JsonProperty(EsClient._TYPE) typeName: String,
  @JsonProperty(EsClient._ID) id: String,
  @JsonProperty(EsClient._VERSION) version: Int,
  created: Boolean
) extends GenericUpdateResponse


/**
  * Case class for an Update document action in Elasticsearch.
  * @param index String ES index name.
  * @param typeName String ES type name.
  * @param id String document id.
  * @param doc String actual document content to update.
  */
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

/**
  * Trait for an Elasticsearch action.
  */
sealed trait Action {
  def index: String
  def typeName: String
  def id: String
}

/**
  * Case class for Elasticsearch Delete action.
  * @param index String ES index name.
  * @param typeName String ES type name.
  * @param id String document id.
  */
case class DeleteAction(index: String, typeName: String, id: String) extends Action

/**
  * Case class for Elasticsearch Update Script action.
  * @param index String ES index name.
  * @param typeName String ES type name.
  * @param id String document id.
  * @param script String ES update script.
  */
case class UpdateScriptAction(index: String, typeName: String, id: String, script: String) extends Action // Update a document using a script
