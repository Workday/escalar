package com.workday.esclient

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.gson.{JsonElement, JsonNull, JsonObject}
import io.searchbox.core._

import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaIteratorConverter}

/**
  * Elasticsearch Query APIs
  */
trait EsQuery extends JestUtils {

  def search(index: String, query: String): EsResult[EsSearchResponse] = {
    search(index, "", query)
  }

  def search(index: String, typeName: String = "", query: String, params: Map[String, Any] = Map()): EsResult[EsSearchResponse] = {
    val jestResult = jest.execute(buildSearchAction(query, index, typeName, params))
    handleSearchResult(jestResult)
  }

  def get(index: String, id: String): EsResult[GetResponse] = {
    val jestResult = jest.execute(buildGetAction(index, id))
    handleJestResult(jestResult) { successfulJestResult =>
      JsonUtils.fromJson[GetResponse](successfulJestResult.getJsonString)
    }
  }

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

  def multiGetSourceField(index: String, typeName: String, ids: Seq[String], field: String): EsResult[MultiGetResponse] = {
    multiGet(index, typeName, ids, Map("_source"->field))
  }

  /**
    * Get count of documents in given index (or alias) with optional filtering by type
    *
    * @param index index/alias name
    * @param typeName optional type (aka sid)
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

  def getSearchHitsCount(index: String, query: String): Int = {
    val jestResult = jest.execute(buildSearchAction(query, index))
    handleSearchResult(jestResult).get.hits.total
  }

  private[this] def handleSearchResult(searchResult: SearchResult): EsResult[EsSearchResponse] = {
    handleJestResult(searchResult) { successfulJestResult =>
      val json = successfulJestResult.getJsonObject

      EsSearchResponse(json.get("took").getAsLong, handleHitsInResult(json),
        if (json.has("aggregations")) json.get("aggregations").getAsJsonObject else new JsonObject
      )
    }
  }

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
          score = h.get(EsClient._SCORE).getAsDouble,
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

    SearchHits(
      total = hitsObj.get("total").getAsInt,
      // hitsobj.get("max_score") might be null or it might be JsonNull. in both cases we want to return None
      maxScore = Option(hitsObj.get("max_score")).flatMap(_ match {
        case _: JsonNull => None
        case hitsElement: JsonElement => Some(hitsElement.getAsDouble)
      }),
      hits = hitsSeq)
  }

  @VisibleForTesting
  private[esclient] def buildSearchAction(query: String, index: String, typeName: String = "", params: Map[String, Any] = Map()): Search = {
    createSearchAction(index, typeName, query, params).build()
  }

  @VisibleForTesting
  private def createSearchAction(index: String, typeName: String = "", query: String, params: Map[String, Any] = Map()) = {
    var searchAction = new Search.Builder(query).addIndex(index)
    if (typeName.nonEmpty) searchAction = searchAction.addType(typeName)
    params.foreach { case (key: String, value: Any) => searchAction.setParameter(key, value) }
    searchAction
  }

  @VisibleForTesting
  private[esclient] def buildGetAction(index: String, id: String): Get =
    new Get.Builder(index, id).build()

  @VisibleForTesting
  private[esclient] def buildMultiGetAction(index: String, typeName: String, ids: Seq[String], params: Map[String, Any] = Map()): MultiGet = {
    val builder = new MultiGet.Builder.ById(index, typeName).addId(ids.asJavaCollection)
    params.foreach { case (key: String, value: Any) => builder.setParameter(key, value) }
    builder.build()
  }
}

trait SearchResponse{
  def took: Long
  def hits: SearchHits
  def aggregations: JsonObject
}

/**
  * We may want to grab more properties in the future
  * @param took the amount of time taken by the query within ES, in milliseconds
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class EsSearchResponse(
  took: Long,
  hits: SearchHits,
  aggregations: JsonObject
) extends SearchResponse

case class ScanAndScrollResponse(
  took: Long,
  hits: SearchHits,
  aggregations: JsonObject,
  scrollID: String
) extends SearchResponse

case class SearchHits(
  total: Int,
  maxScore: Option[Double],
  hits: Seq[SearchHit]
)

case class SearchHit(
  index: String,
  typeName: String,
  id: String,
  score: Double,
  source: String,
  matchedFields: Option[Seq[String]],
  explain: Option[String]
)

/**
  * TODO: version needs to be an Option.  If a doc id is not found, version is missing from the get response.
  * Today, Jackson is giving us 0 for version when the doc is not found.
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

case class MultiGetResponse(
  docs: Seq[GetResponse]
)
