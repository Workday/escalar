/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Builder class for Elasticsearch Cluster Settings list actions.
  */
class ClusterSettingsListBuilder extends AbstractMultiTypeActionBuilder[ClusterSettingsListAction, ClusterSettingsListBuilder] {

  /**
    * Builds a [[com.workday.esclient.actions.ClusterSettingsListAction]].
    * @return [[com.workday.esclient.actions.ClusterSettingsListAction]].
    */
  override def build: ClusterSettingsListAction = new ClusterSettingsListAction(this)
}

/**
  * Cluster settings list action class.
  * @param builder Builder for the action.
  */
class ClusterSettingsListAction(builder: ClusterSettingsListBuilder) extends GenericResultAbstractAction(builder) {
  setURI(buildURI)

  /**
    * Gets the REST method name.
    * @return String of "GET".
    */
  override def getRestMethodName: String = "GET"

  /**
    * Builds the URI for Elasticsearch.
    * @return String of the cluster settings ES URI.
    */
  protected override def buildURI: String = s"_cluster/settings"
}
