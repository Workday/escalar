/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Builder class for [[com.workday.esclient.actions.SnapshotListAction]].
  * @param repository String repository name.
  */
class SnapshotListBuilder(repository: String) extends AbstractMultiTypeActionBuilder[SnapshotListAction, SnapshotListBuilder] {
  val snapshotRepository = repository

  /**
    * Builds [[com.workday.esclient.actions.SnapshotListAction]].
    * @return [[com.workday.esclient.actions.SnapshotListAction]].
    */
  override def build: SnapshotListAction = new SnapshotListAction(this)
}

/**
  * Action class for listing snapshots using the Elasticsearch Snapshot API.
  * @param builder
  */
class SnapshotListAction(builder: SnapshotListBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.snapshotRepository
  setURI(buildURI)

  /**
    * Gets REST method name
    * @return String "GET".
    */
  override def getRestMethodName: String = "GET"

  /**
    * Builds the URI for hitting the list snapshots API.
    * @return String URI.
    */
  protected override def buildURI: String = s"_snapshot/$repository/_all"
}

/**
  * Case class for listing [[com.workday.esclient.actions.SnapshotDefinition]].
  * @param snapshots Sequence of [[com.workday.esclient.actions.SnapshotDefinition]].
  */
case class ListSnapshotResponse(
  snapshots: Seq[SnapshotDefinition]
)

/**
  * Case class for representing a snapshot definition.
  * @param snapshotName String snapshot name.
  * @param indices Sequence of indices in snapshot.
  * @param state String state of snapshot.
  */
case class SnapshotDefinition(
  snapshotName: String,
  indices: Seq[String],
  state: String
)
