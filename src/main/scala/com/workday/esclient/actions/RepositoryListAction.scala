/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Builder class for [[com.workday.esclient.actions.RepositoryListAction]].
  */
class RepositoryListBuilder extends AbstractMultiTypeActionBuilder[RepositoryListAction, RepositoryListBuilder] {
  /**
    * Builds [[com.workday.esclient.actions.RepositoryListAction]].
    * @return [[com.workday.esclient.actions.RepositoryListAction]].
    */
  override def build: RepositoryListAction = new RepositoryListAction(this)
}

/**
  * Action class for listing repositories using the Elasticsearch Snapshot API.
  * @param builder [[com.workday.esclient.actions.RepositoryListBuilder]].
  */
class RepositoryListAction(builder: RepositoryListBuilder) extends GenericResultAbstractAction(builder) {
  setURI(buildURI)

  /**
    * Gets REST method name.
    * @return String "GET".
    */
  override def getRestMethodName: String = "GET"

  /**
    * Builds the URI to hit the list repository API.
    * @return String URI.
    */
  protected override def buildURI: String = s"_snapshot/_all"
}

/**
  * Case class for a List Repository response.
  * @param repositories Sequence of [[com.workday.esclient.actions.RepositoryDefinition]].
  */
case class ListRepositoryResponse(
  repositories: Seq[RepositoryDefinition]
)

/**
  * Case class for Elasticsearch repository definitions.
  * @param repoName String repository name.
  * @param repoType String repository type.
  * @param settings Map any repository settings.
  */
case class RepositoryDefinition(
  repoName: String,
  repoType: String,
  settings: Map[String, Any]
)
