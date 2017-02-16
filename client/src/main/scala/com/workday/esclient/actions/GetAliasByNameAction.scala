/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Action class for the Get Alias by Name Elasticsearch API.
  * @param builder [[com.workday.esclient.actions.GetAliasByNameBuilder]]
  */
class GetAliasByNameAction(builder: GetAliasByNameBuilder) extends GenericResultAbstractAction(builder) {
  val uriSuffix = builder.uriSuffix
  setURI(buildURI)

  /**
    * Gets REST method name.
    * @return String "GET".
    */
  def getRestMethodName: String = "GET"

  /**
    * Builds the URI to hit the Elasticsearch Alias API.
    * @return String alias URI.
    */
  override def buildURI: String = s"_alias/$uriSuffix?format=json&bytes=b"

}

/**
  * Build class for [[com.workday.esclient.actions.GetAliasByNameAction]].
  * @param aliasNames Sequence of alias names.
  */
class GetAliasByNameBuilder(val aliasNames: Seq[String]) extends AbstractMultiTypeActionBuilder[GetAliasByNameAction, GetAliasByNameBuilder] {
  setHeader("content-type", "application/json")
  val uriSuffix = aliasNames.mkString(",")

  /**
    * Builds [[com.workday.esclient.actions.GetAliasByNameAction]].
    * @return [[com.workday.esclient.actions.GetAliasByNameAction]].
    */
  override def build: GetAliasByNameAction = new GetAliasByNameAction(this)
}
