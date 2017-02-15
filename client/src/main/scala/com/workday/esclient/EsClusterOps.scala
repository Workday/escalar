/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.workday.esclient.actions._
import io.searchbox.cluster.{Health, NodesStats, State}
import io.searchbox.core.Cat

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
/**
  * Trait wrapping Elasticsearch Cluster-level APIs
  */
trait EsClusterOps extends JestUtils {

  /**
    * Returns an Elasticsearch cluster health response.
    * @return EsResult wrapping the cluster health response from ES.
    */
  def clusterHealth: EsResult[HealthResponse] = {
    val jestResult = jest.execute(new Health.Builder().build())
    toEsResult[HealthResponse](jestResult)
  }

  /**
    * Returns the least healthy index in the given sequence of Elastichsearch indices.
    * Maps to /_cluster/health/index1,index2,... .
    * @param indices Sequence of ES index names.
    * @param timeout String ES timeout. Defaults to [[com.workday.esclient.actions.IndexHealthAction.DEFAULT_TIMEOUT]]
    * @return Tuple of the least healthy index status and whether the request timed out.
    */
  // performant way to access health of multiple indices, returns the health of the least healthy index from given list of indices
  // --> maps to /_cluster/health/index1,index2,... (see https://www.elastic.co/guide/en/elasticsearch/reference/1.7/cluster-health.html)
  // if an index is missing, this will return as "red" (and "timed_out": true)
  def worstIndexHealth(indices: Seq[String], timeout: String = IndexHealthAction.DEFAULT_TIMEOUT): (String, Boolean) = {
    val jestResult = jest.execute(new IndexHealthBuilder(indices, timeout).build())
    val res = toEsResult[HealthResponse](jestResult).get
    (res.status, res.timedOut)
  }

  /**
    * Cats shard information from Elasticsearch.
    * Includes unassigned shards. Maps to /_cat/shards
    * @param indexName String ES index name. Defaults to empty string.
    * @return EsResult of sequence of [[com.workday.esclient.actions.ShardInfo]].
    */
  def catShards(indexName: String = ""): EsResult[Seq[ShardInfo]] = {
    val getAction = buildCatShards(indexName)
    val jestResult = jest.execute(getAction)
    toEsResult[Seq[ShardInfo]](jestResult)
  }

  /**
    * Reallocates a list of shards to specified destination nodes and returns an acknowledgment from Elasticsearch.
    * Maps to /_cluster/reroute.
    * @param shardAllocation Sequence of RerouteOps containing shard name and destination node.
    * @return EsResult of acknowledgment from ES.
    */
  def allocateShards(shardAllocation: Seq[RerouteOp]): EsResult[RerouteAcknowledgment] = {
    val jestResult = jest.execute(new RerouteBuilder(shardAllocation).build)
    toEsResult[RerouteAcknowledgment](jestResult)
  }

  /**
    * Returns all currently available nodes in the Elasticsearch cluster.
    * Does not include downed nodes. Maps to /_cat/nodes
    * @return EsResult of sequence of [[com.workday.esclient.actions.NodeInfo]].
    */
  def availableNodes: EsResult[Seq[NodeInfo]] = {
    val getAction = buildCatAction(CatAction.CAT_NODES)
    val jestResult = jest.execute(getAction)
    toEsResult[Seq[NodeInfo]](jestResult)
  }

  /**
    * Gets the status for the given Elasticsearch index.
    * @param indexName String ES index name.
    * @return EsResult of sequence of [[com.workday.esclient.actions.IndexInfo]]
    */
  def catIndex(indexName: String) : EsResult[Seq[IndexInfo]] = {
    val catAction = buildCatAction(CatAction.CAT_INDICES, indexName)
    val jestResult = jest.execute(catAction)
    toEsResult[Seq[IndexInfo]](jestResult)
  }

  /**
    * Gets the index status for all Elasticsearch indices.
    * Maps to /_cat/indices
    * @throws com.google.gson.stream.MalformedJsonException
    * @return EsResult of sequence of [[com.workday.esclient.actions.IndexInfo]]
    */
  @throws(classOf[com.google.gson.stream.MalformedJsonException])
  def catAllIndices: EsResult[Seq[IndexInfo]] = {
    val catIndices = buildCatIndices()
    val jestResult = jest.execute(catIndices)
    toEsResult[Seq[IndexInfo]](jestResult)
  }

  // Get basic memory (used heap percentage) and disk (total and available byte counts) stats for all nodes
  def catNodesStats: EsResult[AllNodesStat] = {
    val action = new NodesStats.Builder().withJvm().withFs().build()
    val jestResult = jest.execute(action)
    toEsResult[AllNodesStat](jestResult)
  }
  /**
    * Clear cache keys
    *
    * TODO: add support for specifying which cache (filter, fielddata, etc.) should be cleared
    * https://www.elastic.co/guide/en/elasticsearch/reference/1.7/indices-clearcache.html
    * https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-terms-filter.html
    */
  def clearCacheKeys(keys: Seq[String]): EsResult[ClearCacheResponse] = {
    val builder = new ClearCacheActionBuilder
    if (keys.nonEmpty) builder.filterKeys(keys)
    val jestResult = jest.execute(builder.build)
    toEsResult[ClearCacheResponse](jestResult)
  }

  /**
    * Update settings using /_cluster/settings. If key is not present it will keep its value.
    */
  def updateClusterSettings(transient: java.util.Map[String, String], persistent: java.util.Map[String, String]): EsResult[ClusterSettingsResponse] = {
    val builder = new ClusterSettingsBuilder(transient.asScala.toMap, persistent.asScala.toMap)
    val jestResult = jest.execute(builder.build)
    toEsResult[ClusterSettingsResponse](jestResult)
  }

  /**
    * Get all current cluster settings. Maps to /_cluster/settings.
    */
  def clusterSettings: EsResult[ClusterSettingsResponse] = {
    val builder = new ClusterSettingsListBuilder
    val jestResult = jest.execute(builder.build)
    toEsResult[ClusterSettingsResponse](jestResult)
  }

  def clusterState(indices: Seq[String] = Nil, withRoutingTable: Boolean = false): EsResult[ClusterStateResponse] = {
    val builder = new State.Builder;
    builder.indices(indices.mkString(","))
    if(withRoutingTable)
      builder.withRoutingTable()
    val jestResult = jest.execute(builder.build())
    toEsResult[ClusterStateResponse](jestResult)
  }

  @VisibleForTesting
  private[esclient] def buildCatIndices(): Cat = new Cat.IndicesBuilder().build()

  /**
    * Build a cat action for shards.
    * @param indexName  If specified, only shards in that index are returned.  If not specified, all shards are returned
    */
  @VisibleForTesting
  def buildCatShards(indexName: String = ""): CatAction = {
    buildCatAction(CatAction.CAT_SHARDS, indexName)
  }

  @VisibleForTesting
  private[esclient] def buildCatAction(catAction: String, indexName: String = ""): CatAction = {
    if(indexName.nonEmpty)
      new CatBuilder(catAction, indexName).build
    else
      new CatBuilder(catAction).build
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class HealthResponse(
  clusterName: String,
  status: String,
  timedOut: Boolean,
  numberOfNodes: Int,
  numberOfDataNodes: Int,
  activePrimaryShards: Int,
  activeShards: Int,
  relocatingShards: Int,
  initializingShards: Int,
  unassignedShards: Int
)

case class ClearCacheResponse(@JsonProperty("_shards") shards: ClearCacheShards)
case class ClearCacheShards(total: Int, successful: Int, failed: Int)

@JsonIgnoreProperties(ignoreUnknown = true)
case class ClusterSettingsResponse(transient: Map[String, Any], persistent: Map[String, Any])

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnassignedInfo(
  reason: Option[String],
  at: Option[String],
  details: Option[String]
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class ClusterStateShardInfo(
  state: String,
  primary: Boolean,
  node: Option[String],
  relocatingNode: Option[String],
  shard: Int,
  index: String,
  unassignedInfo: Option[UnassignedInfo]
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class ClusterStateResponse(
  clusterName: String,
  version: Option[Int],
  masterNode: Option[String],
  nodes: Option[JsonNode],
  blocks: Option[JsonNode],
  routingTable: Option[JsonNode],
  metadata: Option[JsonNode]
) {

  def getShards(): Seq[ClusterStateShardInfo] = {
    val shardsInCluster = routingTable.map(_.get("indices").findValues("shards")).map(_.asScala).get
    val resultShardSeq =  shardsInCluster.map(jsonNode => JsonUtils.fromJson[Map[String, Seq[ClusterStateShardInfo]]](jsonNode.toString))
    resultShardSeq.flatMap(_.values.flatten)
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class NodeStat(
  host: String,
  name: String,
  jvm: JsonNode,
  fs: JsonNode,
  attributes: JsonNode
) {
  def jvmHeapUsedPercent: Int = jvm.get("mem").get("heap_used_percent").asInt
  def jvmHeapUsedBytes: Long = jvm.get("mem").get("heap_used_in_bytes").asLong
  def jvmHeapMaxBytes: Long = jvm.get("mem").get("heap_max_in_bytes").asLong
  def diskTotalInBytes: Option[Long] = Option(fs.get("total").get("total_in_bytes")).map(_.asLong)
  def diskAvailableInBytes: Option[Long] = Option(fs.get("total").get("available_in_bytes")).map(_.asLong)
  def getAttribute(attrName: String): Option[String] = Option(attributes.get(attrName)).map(_.textValue)

  def diskUsedInBytes: Option[Long] = {
    if (diskTotalInBytes.isEmpty || diskAvailableInBytes.isEmpty) {
      None
    } else {
      Some(diskTotalInBytes.get - diskAvailableInBytes.get)
    }
  }

  override def toString: String = {
    s"${getClass.getSimpleName}($host,$name,$jvmHeapUsedPercent,$jvmHeapUsedBytes,$jvmHeapMaxBytes," +
      s"$diskTotalInBytes,$diskAvailableInBytes,${attributes.toString})"
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class AllNodesStat(
  nodes: Map[String, NodeStat]
)
