/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import com.google.gson.Gson
import com.workday.esclient.JsonUtils
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

 /**
  * Action class for the Elasticsearch Cluster Update Settings API.
  *  - provides a client API for the ES /_cluster/settings endpoint
  *  - see for more info: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html
  * @param builder [[com.workday.esclient.actions.ClusterSettingsBuilder]]
  */
class ClusterSettings(builder: ClusterSettingsBuilder) extends GenericResultAbstractAction(builder) {
  this.payload = builder.source
  setURI(buildURI)

   /**
     * Gets REST method name.
     * @return String "PUT".
     */
  override def getRestMethodName: String = "PUT"

   /**
     * Builds the URI for the cluster settings API.
     * @return String URI.
     */
  protected override def buildURI: String = s"_cluster/settings"

   /**
     * Gets the payload of the JSON response.
     * @param gson JSON response.
     * @return String of JSON payload.
     */
  override def getData(gson: Gson): String = {
    JsonUtils.toJson(payload)
  }
}

/**
  * Builder class for [[com.workday.esclient.actions.ClusterSettings]].
  * @param transient Map of transient cluster settings updates.
  * @param persistent Map of persistent cluster settings updates.
  */
class ClusterSettingsBuilder(transient: Map[String, String], persistent: Map[String, String]) extends
  AbstractMultiTypeActionBuilder[ClusterSettings, ClusterSettingsBuilder]{
  setHeader("content-type", "application/json")

  val source: Map[String, Any] = Map("persistent" -> persistent, "transient" -> transient)

  /**
    * Builds [[com.workday.esclient.actions.ClusterSettings]].
    * @return [[com.workday.esclient.actions.ClusterSettings]].
    */
  override def build: ClusterSettings = new ClusterSettings(this)
}

