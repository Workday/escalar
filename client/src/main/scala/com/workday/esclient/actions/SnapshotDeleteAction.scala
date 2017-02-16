/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Build for [[com.workday.esclient.actions.SnapshotDeleteAction]].
  * @param repository String repository name.
  * @param name String snapshot name.
  */
class SnapshotDeleteBuilder(repository: String, name: String)
  extends AbstractMultiTypeActionBuilder[SnapshotDeleteAction, SnapshotDeleteBuilder] {
  var indexList : Seq[String] = Nil
  val snapshotRepository = repository
  val snapName = name

  /**
    * Builds [[com.workday.esclient.actions.SnapshotDeleteAction]].
    * @return [[com.workday.esclient.actions.SnapshotDeleteAction]].
    */
  override def build: SnapshotDeleteAction = new SnapshotDeleteAction(this)
}

/**
  * Action class for deleting snapshots using the Elasticsearch Snapshot API.
  * @param builder [[com.workday.esclient.actions.SnapshotDeleteBuilder]].
  */
class SnapshotDeleteAction(builder: SnapshotDeleteBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.snapshotRepository
  val snapName = builder.snapName
  setURI(buildURI)

  /**
    * Gets REST method name.
    * @return String "DELETE".
    */
  override def getRestMethodName: String = "DELETE"

  /**
    * Builds the URI for hitting the Delete Snapshot API.
    * @return String URI.
    */
  protected override def buildURI: String = s"_snapshot/$repository/$snapName"
}
