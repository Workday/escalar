/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Builder class for [[com.workday.esclient.actions.SnapshotRestoreAction]].
  * @param repository String repository name.
  * @param name String name of snapshot.
  * @param wait Boolean whether to wait for action completion.
  */
class SnapshotRestoreBuilder(repository: String, name: String, wait: Boolean = false)
  extends AbstractMultiTypeActionBuilder[SnapshotRestoreAction, SnapshotRestoreBuilder] {
  var indexList : Seq[String] = Nil
  val snapshotRepository = repository
  val snapName = name
  val waitForCompletion = wait

  /**
    * Builds [[com.workday.esclient.actions.SnapshotRestoreAction]].
    * @return [[com.workday.esclient.actions.SnapshotRestoreAction]].
    */
  override def build: SnapshotRestoreAction = new SnapshotRestoreAction(this)
}

/**
  * Action class for restoring snapshots using the Elasticsearch Snapshot API.
  * @param builder [[com.workday.esclient.actions.SnapshotRestoreBuilder]].
  */
class SnapshotRestoreAction(builder: SnapshotRestoreBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.snapshotRepository
  val snapName = builder.snapName
  setURI(buildURI)

  /**
    * Gets the REST method name.
    * @return String "POST".
    */
  override def getRestMethodName: String = "POST"

  /**
    * Builds the URI for hitting the restore snapshots API.
    * @return String URI.
    */
  protected override def buildURI: String = s"_snapshot/${this.repository}/${this.snapName}/_restore?wait_for_completion=${builder.waitForCompletion}"
}
