/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.gson.Gson
import com.workday.esclient.JsonUtils
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

 /**
  * Action class for Elasticsearch Reroute API.
  *  Provides a client API for the ES /_cluster/reroute endpoint.
  *  Enables movement / reassignment of index shards.
  *  See for more info: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-reroute.html
  */
class Reroute(builder: RerouteBuilder, retryFailed: Boolean) extends GenericResultAbstractAction(builder) {
  this.payload = builder.source
  setURI(buildURI)

   /**
     * Gets REST method name.
     * @return String "POST"/
     */
  override def getRestMethodName: String = "POST"

   /**
     * Builds the URI for hitting the Reroute API.
     * @return String URI.
     */
  protected override def buildURI: String = s"_cluster/reroute?retry_failed=$retryFailed"

   /**
     * Gets data for the Reroute action.
     * @param gson [[com.google.gson.Gson]] JSON object.
     * @return String JSON of the reroute action commands.
     */
  override def getData(gson: Gson): String = {
    JsonUtils.toJson(payload)
  }
}

/**
  * Builder class for [[com.workday.esclient.actions.Reroute]].
  * @param rerouteOps Sequence of [[com.workday.esclient.actions.RerouteOp]].
  */
class RerouteBuilder(rerouteOps: Seq[RerouteOp], retryFailed: Boolean) extends AbstractMultiTypeActionBuilder[Reroute, RerouteBuilder]{
  setHeader("content-type", "application/json")

  val source: Map[String, Any] = Map("commands" -> rerouteOps.map(_.toMap))

  /**
    * Builds [[com.workday.esclient.actions.Reroute]].
    * @return [[com.workday.esclient.actions.Reroute]].
    */
  override def build: Reroute = new Reroute(this, retryFailed)
}

/**
  * Trait mapping reroute operation info.
  */
trait RerouteOp {
  def toMap: Map[String, Any]
}

/**
  * Case class for an Allocate Reroute operation.
  * @param index String ES index name.
  * @param shard Int shard number.
  * @param toNode String destination node.
  * @param forcePrimary String whether to allow reroute empty shard, stale shard, or just a regular unassigned
  *                     replica shard. Defaults to true.
  */
case class RerouteAllocate(
  index: String,
  shard: Int,
  toNode: String,
  forcePrimary: String = ""
) extends RerouteOp {

  /**
    * Returns Reroute operation information as a map.
    * @return Map for allocate action.
    */
  override def toMap: Map[String, Any] =
    forcePrimary match {
      case "stale_primary" | "empty_primary" => Map(s"allocate_$forcePrimary" ->
        Map("index" -> index, "shard" -> shard, "node" -> toNode, "accept_data_loss" -> true)
      )
      case _ => Map("allocate_replica" ->
        Map("index" -> index, "shard" -> shard, "node" -> toNode)
      )
    }
}

/**
  * Case class for a Move Reroute operation.
  * @param index String ES index name.
  * @param shard Int shard number.
  * @param fromNode String original node.
  * @param toNode String destination node.
  */
case class RerouteMove(
  index: String,
  shard: Int,
  fromNode: String,
  toNode: String
) extends RerouteOp {

  /**
    * Returns Reroute operation information as a map.
    * @return Map for a move action.
    */
  override def toMap: Map[String, Any] =
    Map("move" ->
      Map("index" -> index, "shard" -> shard,
        "from_node" -> fromNode, "to_node" -> toNode)
    )
}

/**
  * Case class for a Move Reroute operation.
  * @param index String ES index name.
  * @param shard Int shard number.
  * @param node String node.
  */
case class RerouteCancel(
  index: String,
  shard: Int,
  node: String,
  allowPrimary: Boolean = false
) extends RerouteOp {

  /**
    * Returns Reroute operation information as a map.
    * @return Map for a move action.
    */
  override def toMap: Map[String, Any] =
      Map("cancel" ->
        Map("index" -> index, "shard" -> shard,
          "node" -> node, "allow_primary" -> allowPrimary)
      )
}

/**
  * Case class for an Elasticsearch acknowledgment of Reroute operations.
  * @param acknowledged String of acknowledgment.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class RerouteAcknowledgment(
  acknowledged: String
)
