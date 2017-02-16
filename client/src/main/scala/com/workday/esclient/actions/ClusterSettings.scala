/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import com.google.gson.Gson
import com.workday.esclient.JsonUtils
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

 /**
  * Cluster settings class.
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
     * Builds the URI for the Cluster settings API.
     * @return String for ES Cluster settings API.
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
  * Cluster settings Builder class.
  * @param transient Map of transient cluster settings updates.
  * @param persistent Map of persistent cluster settings updates.
  */
class ClusterSettingsBuilder(transient: Map[String, String], persistent: Map[String, String]) extends
  AbstractMultiTypeActionBuilder[ClusterSettings, ClusterSettingsBuilder]{
  setHeader("content-type", "application/json")

  val source: Map[String, Any] = Map("persistent" -> persistent, "transient" -> transient)

  /**
    * Builds the Cluster settings action.
    * @return [[com.workday.esclient.actions.ClusterSettings]]
    */
  override def build: ClusterSettings = new ClusterSettings(this)
}

