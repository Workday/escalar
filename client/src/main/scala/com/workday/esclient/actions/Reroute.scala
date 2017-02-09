package com.workday.esclient.actions

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.gson.Gson
import com.workday.esclient.JsonUtils
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

 /**
  * Reroute Action & Builder
  *  - provides a client API for the ES /_cluster/reroute endpoint
  *  - enables movement / reassignment of index shards
  *  - see for more info: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-reroute.html
  */
class Reroute(builder: RerouteBuilder) extends GenericResultAbstractAction(builder) {
  this.payload = builder.source
  setURI(buildURI)

  override def getRestMethodName: String = "POST"
  protected override def buildURI: String = s"_cluster/reroute"

  override def getData(gson: Gson): String = {
    JsonUtils.toJson(JsonUtils.toJson(payload))
  }
}

class RerouteBuilder(rerouteOps: Seq[RerouteOp]) extends AbstractMultiTypeActionBuilder[Reroute, RerouteBuilder]{
  setHeader("content-type", "application/json")

  val source: Map[String, Any] = Map("commands" -> rerouteOps.map(_.toMap))

  override def build: Reroute = new Reroute(this)
}

trait RerouteOp {
  def toMap: Map[String, Any]
}

case class RerouteAllocate(
  index: String,
  shard: Int,
  toNode: String,
  allowPrimary: Boolean = true
) extends RerouteOp {

  override def toMap: Map[String, Any] =
    Map("allocate" ->
      Map("index" -> index, "shard" -> shard,
        "node" -> toNode, "allow_primary" -> s"$allowPrimary")
    )
}

case class RerouteMove(
  index: String,
  shard: Int,
  fromNode: String,
  toNode: String
) extends RerouteOp {

  override def toMap: Map[String, Any] =
    Map("move" ->
      Map("index" -> index, "shard" -> shard,
        "from_node" -> fromNode, "to_node" -> toNode)
    )
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class RerouteAcknowledgment(
  acknowledged: String
)
