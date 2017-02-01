package com.workday.esclient.actions

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.google.gson.{Gson, JsonObject, JsonParser}
import io.searchbox.action.{AbstractAction, AbstractMultiTypeActionBuilder}
import io.searchbox.core.CatResult

object CatAction {
  val CAT_SHARDS = "shards"
  val CAT_NODES = "nodes"
  val CAT_INDICES = "indices"
}

class CatAction(builder: CatBuilder) extends AbstractAction[CatResult](builder) {
  val PATH_TO_RESULT = "result"
  val operationPath = builder.operationPath
  val uriSuffix = builder.uriSuffix
  val uri = buildURI
  setURI(buildURI)

  def getRestMethodName: String = "GET"

  override def buildURI: String = s"_cat/$operationPath/$uriSuffix?format=json&bytes=b"

  override def getPathToResult: String = PATH_TO_RESULT

  def createNewElasticSearchResult(responseBody: String, statusCode: Int, reasonPhrase: String, gson: Gson): CatResult = {
    createNewElasticSearchResult(new CatResult(gson), responseBody, statusCode, reasonPhrase, gson)
  }

  override def parseResponseBody(responseBody: String): JsonObject = {
    // scalastyle:off null
    if (responseBody != null && !responseBody.trim.isEmpty) {
      // scalastyle:on null
      val parsedJson = new JsonParser().parse(responseBody)
      if (parsedJson.isJsonArray) {
        val result = new JsonObject
        result.add(PATH_TO_RESULT, parsedJson.getAsJsonArray)
        result
      } else {
        parsedJson.getAsJsonObject
      }
    } else {
      new JsonObject
    }
  }
}

class CatBuilder(val operationPath: String, val uriSuffix: String = "") extends AbstractMultiTypeActionBuilder[CatAction, CatBuilder] {
  setHeader("content-type", "application/json")

  override def build: CatAction = new CatAction(this)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class ShardInfo(
  index: String,
  shard: Int,
  state: String,
  node: String,
  docs: Int,
  store: Long,
  prirep: String  // ES flag indicating whether this shard is primary or replica. will be "p" if primary, "r" if replica
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class NodeInfo(
  name: String,
  @JsonProperty("node.role") nodeRole: String
) {
  def isDataNode: Boolean = nodeRole == NodeInfo.DATA_NODE
}


@JsonIgnoreProperties(ignoreUnknown = true)
case class IndexInfo(
  health: String,
  status: String,
  index: String, // Name
  pri: Int,
  rep: Int,
  @JsonProperty("docs.count") docCount: Int,
  @JsonProperty("docs.deleted") docsDeleted: Int,
  @JsonProperty("store.size") storeSize: String,
  @JsonProperty("pri.store.size") priStoreSize: String
)

object NodeInfo {
  val DATA_NODE = "d"
}
