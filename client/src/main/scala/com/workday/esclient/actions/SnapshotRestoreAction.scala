package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

class SnapshotRestoreBuilder(repository: String, name: String, wait: Boolean = false)
  extends AbstractMultiTypeActionBuilder[SnapshotRestoreAction, SnapshotRestoreBuilder] {
  var indexList : Seq[String] = Nil
  val snapshotRepository = repository
  val snapName = name
  val waitForCompletion = wait

  override def build: SnapshotRestoreAction = new SnapshotRestoreAction(this)
}

class SnapshotRestoreAction(builder: SnapshotRestoreBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.snapshotRepository
  val snapName = builder.snapName
  setURI(buildURI)

  override def getRestMethodName: String = "POST"

  protected override def buildURI: String = s"_snapshot/${this.repository}/${this.snapName}/_restore?wait_for_completion=${builder.waitForCompletion}"
}
