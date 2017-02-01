package com.workday.esclient.actions

import com.google.gson.Gson
import com.workday.esclient.JsonUtils
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

 /**
  * Cluster settings Action & Builder
  *  - provides a client API for the ES /_cluster/settings endpoint
  *  - see for more info: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html
  */
class ClusterSettings(builder: ClusterSettingsBuilder) extends GenericResultAbstractAction(builder) {
  this.payload = builder.source
  setURI(buildURI)

  override def getRestMethodName: String = "PUT"
  protected override def buildURI: String = s"_cluster/settings"

  override def getData(gson: Gson): String = {
    JsonUtils.toJson(payload)
  }
}

class ClusterSettingsBuilder(transient: Map[String, String], persistent: Map[String, String]) extends
  AbstractMultiTypeActionBuilder[ClusterSettings, ClusterSettingsBuilder]{
  setHeader("content-type", "application/json")

  val source: Map[String, Any] = Map("persistent" -> persistent, "transient" -> transient)

  override def build: ClusterSettings = new ClusterSettings(this)
}

