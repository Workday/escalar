/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.gson.{JsonElement, JsonNull, JsonObject}
import io.searchbox.core._

import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaIteratorConverter}

/**
  * Trait wrapping Elasticsearch Query APIs
  */
trait EsQuery extends JestUtils {

  /**
    * Makes a basic query on Elasticsearch and returns an EsResult of the response.
    * @param index String ES index to query.
    * @param query String query content.
    * @return EsResult wrapping the response from ES.
    */
  def search(index: String, query: String): EsResult[EsSearchResponse] = {
    search(index, "", query)
  }

  /**
    * Makes a more sophisticated query on Elasticsearch including ES type names and parameters.
    * @param index String ES index to query.
    * @param typeName String ES type name.
    * @param query String query content.
    * @param params Map of params to include in ES request.
    * @return EsResult wrapping the response from ES.
    */
  def search(index: String, typeName: String = "", query: String, params: Map[String, Any] = Map()): EsResult[EsSearchResponse] = {
    val jestResult = jest.execute(buildSearchAction(query, index, typeName, params))
    handleSearchResult(jestResult)
  }

  /**
    * Makes a get request to Elasticsearch for the given id and index.
    * @param index String index to get from.
    * @param id String id of document to get.
    * @return EsResult wrapping the response from ES.
    */
  def get(index: String, id: String): EsResult[GetResponse] = {
    val jestResult = jest.execute(buildGetAction(index, id))
    handleJestResult(jestResult) { successfulJestResult =>
      JsonUtils.fromJson[GetResponse](successfulJestResult.getJsonString)
    }
  }

  /**
    * Makes a multi-get request to Elasticsearch for the given index and ids.
    * @param index String index to get from.
    * @param typeName String ES type name.
    * @param ids Sequence of ids of documents to get.
    * @param params Optional map of additional get parameters.
    * @return EsResult wrapping the response from ES.
    */
  def multiGet(index: String, typeName: String, ids: Seq[String], params: Map[String, Any] = Map()): EsResult[MultiGetResponse] = {
    // don't bother hitting elastic search if there are no id's
    if (ids.isEmpty){
      new EsResponse[MultiGetResponse](new MultiGetResponse(Nil))
    } else {
      val jestResult = jest.execute(buildMultiGetAction(index, typeName, ids.distinct, params))
      handleJestResult(jestResult, allowError = false) { successfulJestResult =>
        val parsedResult = JsonUtils.fromJson[MultiGetResponse](successfulJestResult.getJsonString)
        val docsIterator = successfulJestResult.getJsonObject.getAsJsonArray("docs").iterator.asScala
        def idAndSourcePair(jsonElement: JsonElement): (String, (Option[String], Option[String])) = {
          val jsonObject = jsonElement.getAsJsonObject
          val id = jsonObject.get(EsClient._ID)
          val source = Option(jsonObject.get(EsClient._SOURCE)).map(_.toString)
          val error = Option(jsonObject.get(EsClient.ERROR)).map(_.getAsString)
          id.getAsString ->(source, error)
        }
        val sourcesMappedById: Map[String, (Option[String], Option[String])] = docsIterator.map(idAndSourcePair).toMap
        def copyWithNewSource(getResponse: GetResponse): GetResponse = {
          sourcesMappedById(getResponse.id) match {
            case (source: Option[String], error: Option[String]) =>
              getResponse.copy(sourceIn = source, error = error)
          }
        }
        new MultiGetResponse(parsedResult.docs map copyWithNewSource)
      }
    }
  }

  /**
    * Makes a Multi Get request with the "_source" field provided as a parameter.
    * @param index String index to get from.
    * @param typeName String ES type name.
    * @param ids Sequence of ids of documents to get.
    * @param field String "_source" field to specify which documents to return source for.
    * @return EsResult wrapping the response from ES.
    */
  def multiGetSourceField(index: String, typeName: String, ids: Seq[String], field: String): EsResult[MultiGetResponse] = {
    multiGet(index, typeName, ids, Map("_source"->field))
  }

  /**
    * Gets the count of documents in a given index (or alias) with optional filtering by type.
    * @param index String index/alias name to get from.
    * @param typeName String optional ES type name (aka sid).
    * @return EsResult wrapping an integer count from ES.
    */
  def count(index: String, typeName: Option[String] = None): EsResult[Int] = {
    val indexBuilder = new Count.Builder().addIndex(index)
    val builder = typeName match {
      case Some(t) => indexBuilder.addType(t)
      case None => indexBuilder
    }
    val jestResult = jest.execute(builder.build())
    handleJestResult(jestResult) { _.getCount.toInt }
  }

  /**
    * Gets the count of search hits for a given query and index.
    * @param index String index to get from.
    * @param query String query content.
    * @return Int count of hits for given query.
    */
  def getSearchHitsCount(index: String, query: String): Int = {
    val jestResult = jest.execute(buildSearchAction(query, index))
    handleSearchResult(jestResult).get.hits.total
  }

  /**
    * Handles the Jest result from an ES request and returns a JSON object.
    * @param searchResult Jest SearchResult from ES.
    * @return EsResult wrapping the handled response from ES.
    */
  private[this] def handleSearchResult(searchResult: SearchResult): EsResult[EsSearchResponse] = {
    handleJestResult(searchResult) { successfulJestResult =>
      val json = successfulJestResult.getJsonObject

      EsSearchResponse(json.get("took").getAsLong, handleHitsInResult(json),
        if (json.has("aggregations")) json.get("aggregations").getAsJsonObject else new JsonObject
      )
    }
  }

  /**
    * Parses JSON search response and maps hits to SearchHit instances.
    * These instances have Index, Type, ID, Score, Source, and Matched fields.
    * @param searchJson JsonObject representing a search response from ES.
    * @return SearchHits object with hits data.
    */
  // scalastyle:off cyclomatic.complexity
  @VisibleForTesting
  private[esclient] def handleHitsInResult(searchJson: JsonObject): SearchHits = {
    val hitsObj = searchJson.get("hits").getAsJsonObject
    val hitsSeq = if (hitsObj.has("hits")) {
      hitsObj.get("hits").getAsJsonArray.iterator().asScala.toSeq.map(hit => {
        val h = hit.getAsJsonObject
        SearchHit(
          // index and typeName are sometimes left out for performance reasons with large result sets
          index = if (h.has(EsClient._INDEX)) h.get(EsClient._INDEX).getAsString else "",
          typeName = if (h.has(EsClient._TYPE)) h.get(EsClient._TYPE).getAsString else "",
          id = h.get(EsClient._ID).getAsString,
          score = if (h.has(EsClient._SCORE) && !h.get(EsClient._SCORE).isJsonNull)
            Some(h.get(EsClient._SCORE).getAsDouble) else None,
          source = if (h.has(EsClient._SOURCE)) h.get(EsClient._SOURCE).toString else "", // getting all doc IDs does not give back any fields
          matchedFields = {
            if (h.has(EsClient.MATCHED_QUERIES))
              Some(h.get(EsClient.MATCHED_QUERIES).getAsJsonArray.iterator().asScala.toSeq.map {
                element =>
                  // remove the named query delimiter and everything after it, if it exists
                  val queryName = element.getAsString
                  val delimIndex = queryName.indexOf(EsNames.NAME_QUERY_DELIMITER)
                  if (delimIndex >= 0) queryName.substring(0, delimIndex) else queryName
              })
            else
              None
          },
          explain = {
            if (h.has(EsClient._EXPLANATION)) {
              Some(h.get(EsClient._EXPLANATION).toString)
            } else {
              None
            }
          }
        )
      })
    } else {
      Seq.empty
    }
    // scalastyle:on cyclomatic.complexity

    SearchHits(
      total = hitsObj.get("total").getAsInt,
      // hitsobj.get("max_score") might be null or it might be JsonNull. in both cases we want to return None
      maxScore = Option(hitsObj.get("max_score")).flatMap(_ match {
        case _: JsonNull => None
        case hitsElement: JsonElement => Some(hitsElement.getAsDouble)
      }),
      hits = hitsSeq)
  }

  /**
    * Builds a Search object for use with a Jest client.
    * @param query String query content.
    * @param index String index to search from.
    * @param typeName String optional ES type name.
    * @param params Optional map of additional search parameters.
    * @return Search object.
    */
  @VisibleForTesting
  private[esclient] def buildSearchAction(query: String, index: String, typeName: String = "", params: Map[String, Any] = Map()): Search = {
    createSearchAction(index, typeName, query, params).build()
  }

  /**
    * Returns a buildable search action for us with a Jest client.
    * @param index String index to search from.
    * @param typeName String optional ES type name.
    * @param query String query content.
    * @param params Optional map of additional search parameters.
    * @return buildable search action object.
    */
  @VisibleForTesting
  private def createSearchAction(index: String, typeName: String = "", query: String, params: Map[String, Any] = Map()) = {
    var searchAction = new Search.Builder(query).addIndex(index)
    if (typeName.nonEmpty) searchAction = searchAction.addType(typeName)
    params.foreach { case (key: String, value: Any) => searchAction.setParameter(key, value) }
    searchAction
  }

  /**
    * Returns a Get object for use with a Jest client.
    * @param index String index to get from.
    * @param id String id of document to get.
    * @return Get object.
    */
  @VisibleForTesting
  private[esclient] def buildGetAction(index: String, id: String): Get =
    new Get.Builder(index, id).build()

  /**
    * Returns a MultiGet object for use with a Jest client.
    * @param index String index to get from.
    * @param typeName String ES type name.
    * @param ids Sequence of ids of documents to get.
    * @param params Optional map of additional parameters.
    * @return MultiGet object.
    */
  @VisibleForTesting
  private[esclient] def buildMultiGetAction(index: String, typeName: String, ids: Seq[String], params: Map[String, Any] = Map()): MultiGet = {
    val builder = new MultiGet.Builder.ById(index, typeName).addId(ids.asJavaCollection)
    params.foreach { case (key: String, value: Any) => builder.setParameter(key, value) }
    builder.build()
  }
}

/**
  * Search Response trait for wrapping ES response data
  */
trait SearchResponse{
  def took: Long
  def hits: SearchHits
  def aggregations: JsonObject
}

/**
  * Case class of SearchResponse for ES responses
  * We may want to grab more properties in the future
  * @param took Long the amount of time taken by the query within ES, in milliseconds
  * @param hits SearchHits from ES.
  * @param aggregations JsonObject for any search aggregations.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class EsSearchResponse(
  took: Long,
  hits: SearchHits,
  aggregations: JsonObject
) extends SearchResponse

/**
  * Case class for a scan and scroll response
  * @param took Long the amount of time taken by the query within ES, in milliseconds
  * @param hits SearchHits from ES.
  * @param aggregations JsonObject for any search aggregations.
  * @param scrollID String id for retrieving next batch of results.
  */
case class ScanAndScrollResponse(
  took: Long,
  hits: SearchHits,
  aggregations: JsonObject,
  scrollID: String
) extends SearchResponse

/**
  * Case class for wrapping search hits from ES.
  * @param total Int total number of hits.
  * @param maxScore Double max score of the hits.
  * @param hits Sequence of actual SearchHit objects.
  */
case class SearchHits(
  total: Int,
  maxScore: Option[Double],
  hits: Seq[SearchHit]
)

/**
  * Case class for wrapping an individual search hit from ES.
  * @param index String ES index.
  * @param typeName String ES type name.
  * @param id String id of document.
  * @param score Double score of document.
  * @param source String actual document source.
  * @param matchedFields Optional sequence of matched fields
  * @param explain Optional string of ES explanations for the document.
  */
case class SearchHit(
  index: String,
  typeName: String,
  id: String,
  score: Option[Double],
  source: String,
  matchedFields: Option[Seq[String]],
  explain: Option[String]
)

/**
  * Case class for a Get response from Elasticsearch.
  * TODO: version needs to be an Option.  If a doc id is not found, version is missing from the get response.
  * Today, Jackson is giving us 0 for version when the doc is not found.
  * @param index String ES index.
  * @param typeName String ES type name.
  * @param id String document id.
  * @param version Int version for the document.
  * @param sourceJson JsonNode representing original source.
  * @param sourceIn Optional string for source. Defaults to None.
  * @param found Boolean whether document was found by ES.
  * @param error Optional string for an ES error response.
  */
// Mapping for ES Get API (https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html)
case class GetResponse(
  @JsonProperty(EsClient._INDEX) index: String,
  @JsonProperty(EsClient._TYPE) typeName: String,
  @JsonProperty(EsClient._ID) id: String,
  @JsonProperty(EsClient._VERSION) version: Int,
  @JsonProperty(EsClient._SOURCE) sourceJson: Option[JsonNode], // can't be mapped directly to String b/c Jackson will parse incorrectly
  sourceIn: Option[String] = None,
  found: Boolean,
  error: Option[String] = None
) {
  lazy val source: Option[String] = sourceJson.map(JsonUtils.toJson(_)).orElse(sourceIn) // convert json to string
  def foundVersion: Option[Int] = if (found) Some(version) else None
}

/**
  * Case class for a MultiGet response from Elasticsearch.
  * @param docs Sequence of Get responses.
  */
case class MultiGetResponse(
  docs: Seq[GetResponse]
)
