package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

class SnapshotDeleteBuilder(repository: String, name: String)
  extends AbstractMultiTypeActionBuilder[SnapshotDeleteAction, SnapshotDeleteBuilder] {
  var indexList : Seq[String] = Nil
  val snapshotRepository = repository
  val snapName = name

  override def build: SnapshotDeleteAction = new SnapshotDeleteAction(this)
}

class SnapshotDeleteAction(builder: SnapshotDeleteBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.snapshotRepository
  val snapName = builder.snapName
  setURI(buildURI)

  override def getRestMethodName: String = "DELETE"

  protected override def buildURI: String = s"_snapshot/$repository/$snapName"
}
