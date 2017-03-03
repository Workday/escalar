/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Build class for [[com.workday.esclient.actions.RepositoryDeleteAction]].
  * @param repositoryName String ES repository name.
  */
class RepositoryDeleteBuilder(repositoryName: String)
  extends AbstractMultiTypeActionBuilder[RepositoryDeleteAction, RepositoryDeleteBuilder] {
  val repoName = repositoryName

  /**
    * Builds [[com.workday.esclient.actions.RepositoryDeleteAction]].
    * @return [[com.workday.esclient.actions.RepositoryDeleteAction]].
    */
  override def build: RepositoryDeleteAction = new RepositoryDeleteAction(this)
}

/**
  * Action class for deleting repositories using the Elasticsearch Snapshot API.
  * @param builder [[com.workday.esclient.actions.RepositoryDeleteBuilder]].
  */
class RepositoryDeleteAction(builder: RepositoryDeleteBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.repoName
  setURI(buildURI)

  /**
    * Gets REST method name.
    * @return String "DELETE".
    */
  override def getRestMethodName: String = "DELETE"

  /**
    * Builds the URI to hit the delete repository API.
    * @return String URI.
    */
  protected override def buildURI: String = s"_snapshot/$repository"
}
