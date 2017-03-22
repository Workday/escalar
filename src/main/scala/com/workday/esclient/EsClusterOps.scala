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
    toEsResult[Seq[IndexInfo_1_7]](jestResult)
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
    toEsResult[Seq[IndexInfo_1_7]](jestResult)
  }

  /**
    * Gets the basic memory and disk stats for all Elasticsearch nodes.
    * Memory stats -> used heap percentage; Disk stats -> total and available byte counts.
    * @return EsResult of [[com.workday.esclient.AllNodesStat]]
    */
  def catNodesStats: EsResult[AllNodesStat] = {
    val action = new NodesStats.Builder().withJvm().withFs().build()
    val jestResult = jest.execute(action)
    toEsResult[AllNodesStat](jestResult)
  }
  /**
    * Clears Elasticsearch cache keys.
    * https://www.elastic.co/guide/en/elasticsearch/reference/1.7/indices-clearcache.html
    * https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-terms-filter.html
    * @param keys Sequence of cache keys to clear.
    * @return EsResult of [[com.workday.esclient.ClearCacheResponse]]
    */
  //TODO: add support for specifying which cache (filter, fielddata, etc.) should be cleared
  def clearCacheKeys(keys: Seq[String]): EsResult[ClearCacheResponse] = {
    val builder = new ClearCacheActionBuilder
    if (keys.nonEmpty) builder.filterKeys(keys)
    val jestResult = jest.execute(builder.build)
    toEsResult[ClearCacheResponse](jestResult)
  }

  /**
    * Updates Elasticsearch cluster settings.
    * Maps to /_cluster/settings. If key is not present it will keep its value.
    * @param transient Map of transient settings to update. Will not survive a cluster restart.
    * @param persistent Map of persistent settings to update. Persist across cluster restarts.
    * @return EsResult of [[com.workday.esclient.ClusterSettingsResponse]]
    */
  def updateClusterSettings(transient: java.util.Map[String, String], persistent: java.util.Map[String, String]): EsResult[ClusterSettingsResponse] = {
    val builder = new ClusterSettingsBuilder(transient.asScala.toMap, persistent.asScala.toMap)
    val jestResult = jest.execute(builder.build)
    toEsResult[ClusterSettingsResponse](jestResult)
  }

  /**
    * Gets all current Elasticsearch cluster settings.
    * Maps to /_cluster/settings.
    * @return EsResult of [[com.workday.esclient.ClusterSettingsResponse]]
    */
  def clusterSettings: EsResult[ClusterSettingsResponse] = {
    val builder = new ClusterSettingsListBuilder
    val jestResult = jest.execute(builder.build)
    toEsResult[ClusterSettingsResponse](jestResult)
  }

  /**
    * Gets the current state of the Elasticsearch cluster.
    * Maps to /_cluster/state.
    * @param indices Sequence of indices to filter cluster state response with. Defaults to Nil.
    * @param withRoutingTable Boolean whether to include routing table in the cluster state response. Defaults to false.
    * @return EsResult of [[com.workday.esclient.ClusterStateResponse]]
    */
  def clusterState(indices: Seq[String] = Nil, withRoutingTable: Boolean = false): EsResult[ClusterStateResponse] = {
    val builder = new State.Builder;
    builder.indices(indices.mkString(","))
    if(withRoutingTable)
      builder.withRoutingTable()
    val jestResult = jest.execute(builder.build())
    toEsResult[ClusterStateResponse](jestResult)
  }

  /**
    * Returns a Cat action for indices.
    * @return Cat action.
    */
  @VisibleForTesting
  def buildCatIndices(): Cat = new Cat.IndicesBuilder().build()

  /**
    * Builds a Cat action for shards.
    * @param indexName If specified, only shards in that index are returned.  If not specified, all shards are returned.
    * @return [[com.workday.esclient.actions.CatAction]].
    */
  @VisibleForTesting
  def buildCatShards(indexName: String = ""): CatAction = {
    buildCatAction(CatAction.CAT_SHARDS, indexName)
  }

  /**
    * Builds a Cat action.
    * @param catAction String of cat action to build.
    * @param indexName String of ES index name. Defaults to empty string.
    * @return [[com.workday.esclient.actions.CatAction]]
    */
  @VisibleForTesting
  private[esclient] def buildCatAction(catAction: String, indexName: String = ""): CatAction = {
    if(indexName.nonEmpty)
      new CatBuilder(catAction, indexName).build
    else
      new CatBuilder(catAction).build
  }
}

/**
  * Case class for an Elasticsearch cluster health response.
  * @param clusterName String cluster name.
  * @param status String cluster status.
  * @param timedOut Boolean whether cluster timed out.
  * @param numberOfNodes Int number of nodes in cluster.
  * @param numberOfDataNodes Int number of data nodes.
  * @param activePrimaryShards Int number of primary shards.
  * @param activeShards Int number of active shards.
  * @param relocatingShards Int number of reallocating shards.
  * @param initializingShards Int number of initializing shards.
  * @param unassignedShards Int number of unassigned shards.
  */
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

/**
  * Case class for an Elasticsearch clear cache response.
  * @param shards [[com.workday.esclient.ClearCacheShards]]
  */
case class ClearCacheResponse(@JsonProperty("_shards") shards: ClearCacheShards)

/**
  * Case class for cleared cache shards.
  * @param total Int total number of cleared shards.
  * @param successful Int number of successfully cleared shards.
  * @param failed Int number of failed shards.
  */
case class ClearCacheShards(total: Int, successful: Int, failed: Int)

/**
  * Case class for an Elasticsearch cluster settings response.
  * @param transient Map of transient settings updates.
  * @param persistent Map of persistent settings updates.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class ClusterSettingsResponse(transient: Map[String, Any], persistent: Map[String, Any])

/**
  * Case class wrapping the unassigned info field of Elasticsearch cluster shard info.
  * @param reason String reason code given for unassigned shard.
  * @param at String for node name.
  * @param details String for details of failure.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnassignedInfo(
  reason: Option[String],
  at: Option[String],
  details: Option[String]
)

/**
  * Case class wrapping Elasticsearch cluster state shard information.
  * @param state String shard state.
  * @param primary Boolean whether shard is primary or not.
  * @param node String node name.
  * @param relocatingNode String relocating node name.
  * @param shard Int shard id.
  * @param index String index in shard.
  * @param unassignedInfo [[com.workday.esclient.UnassignedInfo]]
  */
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

/**
  * Case class for an Elasticsearch cluster state response.
  * @param clusterName String cluster name.
  * @param version Int for the cluster version.
  * @param masterNode String name of master node.
  * @param nodes [[com.fasterxml.jackson.databind.JsonNode]] for the nodes in cluster.
  * @param blocks [[com.fasterxml.jackson.databind.JsonNode]] for blocks in cluster.
  * @param routingTable [[com.fasterxml.jackson.databind.JsonNode]] cluster routing table.
  * @param metadata [[com.fasterxml.jackson.databind.JsonNode]] cluster metadata.
  */
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

  /**
    * Gets all shards in cluster and returns a sequence of [[com.workday.esclient.ClusterStateShardInfo]].
    * @return [[com.workday.esclient.ClusterStateShardInfo]]
    */
  def getShards(): Seq[ClusterStateShardInfo] = {
    val shardsInCluster = routingTable.map(_.get("indices").findValues("shards")).map(_.asScala).get
    val resultShardSeq =  shardsInCluster.map(jsonNode => JsonUtils.fromJson[Map[String, Seq[ClusterStateShardInfo]]](jsonNode.toString))
    resultShardSeq.flatMap(_.values.flatten)
  }
}

/**
  * Case class for Elasticsearch node statistics.
  * @param host String hostname.
  * @param name String node name.
  * @param jvm [[com.fasterxml.jackson.databind.JsonNode]] of JVM stats like memory pool info, GC, and buffer pools.
  * @param fs [[com.fasterxml.jackson.databind.JsonNode]] of file system information like data path, read/write stats, etc.
  * @param attributes [[com.fasterxml.jackson.databind.JsonNode]] of node attributes.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class NodeStat(
  host: String,
  name: String,
  jvm: JsonNode,
  fs: JsonNode,
  attributes: JsonNode
) {
  /**
    * Returns JVM heap usage by percent.
    * @return Int of JVM heap usage.
    */
  def jvmHeapUsedPercent: Int = jvm.get("mem").get("heap_used_percent").asInt

  /**
    * Returns JVM heap usage in bytes.
    * @return Long of JVM heap usage.
    */
  def jvmHeapUsedBytes: Long = jvm.get("mem").get("heap_used_in_bytes").asLong

  /**
    * Returns JVM heap max bytes.
    * @return Long of JVM max usage.
    */
  def jvmHeapMaxBytes: Long = jvm.get("mem").get("heap_max_in_bytes").asLong

  /**
    * Returns disk total space in bytes.
    * @return Optional long of total disk space.
    */
  def diskTotalInBytes: Option[Long] = Option(fs.get("total").get("total_in_bytes")).map(_.asLong)

  /**
    * Returns available disk space in bytes.
    * @return Optional long of available disk space.
    */
  def diskAvailableInBytes: Option[Long] = Option(fs.get("total").get("available_in_bytes")).map(_.asLong)

  /**
    * Gets a node attribute by name.
    * @param attrName String attribute name.
    * @return Optional string value of attribute.
    */
  def getAttribute(attrName: String): Option[String] = Option(attributes.get(attrName)).map(_.textValue)

  /**
    * Returns disk usage in bytes.
    * @return Optional long of disk usage.
    */
  def diskUsedInBytes: Option[Long] = {
    if (diskTotalInBytes.isEmpty || diskAvailableInBytes.isEmpty) {
      None
    } else {
      Some(diskTotalInBytes.get - diskAvailableInBytes.get)
    }
  }

  /**
    * Returns string representation of object.
    * @return String representation of object.
    */
  override def toString: String = {
    s"${getClass.getSimpleName}($host,$name,$jvmHeapUsedPercent,$jvmHeapUsedBytes,$jvmHeapMaxBytes," +
      s"$diskTotalInBytes,$diskAvailableInBytes,${attributes.toString})"
  }
}

/**
  * Case class wrapping all node stats in cluster.
  * @param nodes Map of all nodes in cluster as [[com.workday.esclient.NodeStat]]
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class AllNodesStat(
  nodes: Map[String, NodeStat]
)
