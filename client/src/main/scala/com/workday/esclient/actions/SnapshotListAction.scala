package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

class SnapshotListBuilder(repository: String) extends AbstractMultiTypeActionBuilder[SnapshotListAction, SnapshotListBuilder] {
  val snapshotRepository = repository

  override def build: SnapshotListAction = new SnapshotListAction(this)
}

class SnapshotListAction(builder: SnapshotListBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.snapshotRepository
  setURI(buildURI)

  override def getRestMethodName: String = "GET"

  protected override def buildURI: String = s"_snapshot/$repository/_all"
}

case class ListSnapshotResponse(
  snapshots: Seq[SnapshotDefinition]
)

case class SnapshotDefinition(
  snapshotName: String,
  indices: Seq[String],
  state: String
)
