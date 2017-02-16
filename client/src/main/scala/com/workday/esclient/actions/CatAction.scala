/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.google.gson.{Gson, JsonObject, JsonParser}
import io.searchbox.action.{AbstractAction, AbstractMultiTypeActionBuilder}
import io.searchbox.core.CatResult

/**
  * CatAction object for string constants.
  */
object CatAction {
  val CAT_SHARDS = "shards"
  val CAT_NODES = "nodes"
  val CAT_INDICES = "indices"
}

/**
  * Cat action wrapper class for Elasticsearch.
  * @param builder Build object.
  */
class CatAction(builder: CatBuilder) extends AbstractAction[CatResult](builder) {
  val PATH_TO_RESULT = "result"
  val operationPath = builder.operationPath
  val uriSuffix = builder.uriSuffix
  val uri = buildURI
  setURI(buildURI)

  /**
    * Gets REST method name.
    * @return String "GET".
    */
  def getRestMethodName: String = "GET"

  /**
    * Builds URI for Cat actions.
    * @return String URI to pass to Elasticsearch.
    */
  override def buildURI: String = s"_cat/$operationPath/$uriSuffix?format=json&bytes=b"

  /**
    * Gets string constant for Elasticsearch results.
    * @return String [[com.workday.esclient.actions.CatAction.PATH_TO_RESULT]]
    */
  override def getPathToResult: String = PATH_TO_RESULT

  /**
    * Creates a new Elasticsearch result.
    * @param responseBody String ES response body.
    * @param statusCode Int status code from ES.
    * @param reasonPhrase String reason given by ES for status code.
    * @param gson Gson JSON object.
    * @return [[io.searchbox.core.CatResult]] from ES.
    */
  def createNewElasticSearchResult(responseBody: String, statusCode: Int, reasonPhrase: String, gson: Gson): CatResult = {
    createNewElasticSearchResult(new CatResult(gson), responseBody, statusCode, reasonPhrase, gson)
  }

  /**
    * Parses JSON response body from Elasticsearch.
    * @param responseBody String JSON response body.
    * @return [[com.google.gson.JsonObject]] of ES results.
    */
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

/**
  * Builder class for Elasticsearch Cat actions.
  * @param operationPath String path to ES operation.
  * @param uriSuffix String ES URI suffix.
  */
class CatBuilder(val operationPath: String, val uriSuffix: String = "") extends AbstractMultiTypeActionBuilder[CatAction, CatBuilder] {
  setHeader("content-type", "application/json")

  /**
    * Builds a Cat action.
    * @return [[com.workday.esclient.actions.CatAction]]
    */
  override def build: CatAction = new CatAction(this)
}

/**
  * Case class for Elasticsearch shard info.
  * @param index String ES index name.
  * @param shard Int shard number.
  * @param state String state of shard.
  * @param node String ES node name.
  * @param docs Int number of docs in shard.
  * @param store Long storage size in shard.
  * @param prirep String ES flag indicating whether this shard is primary or replica. will be "p" if primary, "r" if replica
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class ShardInfo(
  index: String,
  shard: Int,
  state: String,
  node: String,
  docs: Int,
  store: Long,
  prirep: String
)

/**
  * Case class for Elasticsearch node information.
  * @param name String ES node name.
  * @param nodeRole String node role.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class NodeInfo(
  name: String,
  @JsonProperty("node.role") nodeRole: String
) {
  /**
    * Returns whether node is a data node.
    * @return Boolean whether node is data node.
    */
  def isDataNode: Boolean = nodeRole == NodeInfo.DATA_NODE
}

/**
  * Case class for Elasticsearch index information.
  * @param health String ES index health.
  * @param status String index status.
  * @param index String index name.
  * @param pri Int number of primary shards.
  * @param rep Int number of replica shards.
  * @param docCount Int number of docs in index.
  * @param docsDeleted Int number of deleted docs.
  * @param storeSize String storage size of index.
  * @param priStoreSize String storage size of primary shards.
  */
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

/**
  * Node information object for data node string constant.
  */
object NodeInfo {
  val DATA_NODE = "d"
}
