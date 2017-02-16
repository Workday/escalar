/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.cluster.Health

/**
  * Utility object for storing Index Health action string constants.
  */
object IndexHealthAction {
  val DEFAULT_TIMEOUT = "50ms"
}

/**
  * Action class for the Elasticsearch Index Health API.
  * @param builder [[com.workday.esclient.actions.IndexHealthBuilder]].
  */
class IndexHealthAction(builder: IndexHealthBuilder) extends Health(builder) {
  /**
    * Builds the URI to hit the Elasticsearch Index Health API.
    * @return String URI.
    */
  protected override def buildURI: String = super.buildURI + builder.indices.mkString(",") + "?timeout=" + builder.timeout
}

/**
  * Builder class for [[com.workday.esclient.actions.IndexHealthAction]]
  * @param indices Sequence of ES indices.
  * @param timeout String timeout value. Defaults to [[com.workday.esclient.actions.IndexHealthAction.DEFAULT_TIMEOUT]]
  */
class IndexHealthBuilder(val indices: Seq[String] = Nil, val timeout: String = IndexHealthAction.DEFAULT_TIMEOUT) extends Health.Builder {
  /**
    * Builds [[com.workday.esclient.actions.IndexHealthAction]].
    * @return [[com.workday.esclient.actions.IndexHealthAction]].
    */
  override def build: IndexHealthAction = new IndexHealthAction(this)
}
