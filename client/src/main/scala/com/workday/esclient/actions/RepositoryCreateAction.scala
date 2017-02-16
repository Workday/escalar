/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import com.google.gson.Gson
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Builder class for [[com.workday.esclient.actions.RepositoryCreateAction]].
  * @param repositoryName String ES repository name.
  * @param repositoryDefinition String ES repository definition.
  */
class RepositoryCreateBuilder(repositoryName: String, repositoryDefinition: String)
  extends AbstractMultiTypeActionBuilder[RepositoryCreateAction, RepositoryCreateBuilder] {
  val repository = repositoryName
  val definition = repositoryDefinition

  /**
    * Builds [[com.workday.esclient.actions.RepositoryCreateAction]].
    * @return [[com.workday.esclient.actions.RepositoryCreateAction]].
    */
  override def build: RepositoryCreateAction = new RepositoryCreateAction(this)
}

/**
  * Action class for Create Repository Elasticsearch API.
  * @param builder [[com.workday.esclient.actions.RepositoryCreateBuilder]].
  */
class RepositoryCreateAction(builder: RepositoryCreateBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.repository
  val definition = builder.definition
  setURI(buildURI)

  /**
    * Gets REST method name.
    * @return String "PUT".
    */
  override def getRestMethodName: String = "PUT"

  /**
    * Gets data about the ES repository.
    * @param gson [[com.google.gson.Gson]] JSON object.
    * @return String [[com.workday.esclient.actions.RepositoryCreateAction.definition]]
    */
  override def getData(gson: Gson): String = definition

  /**
    * Builds the URI to use the ES Snapshot API.
    * @return Sting URI.
    */
  protected override def buildURI: String = s"_snapshot/$repository"
}
