package com.workday.esclient.actions

import com.google.gson.Gson
import com.workday.esclient.JsonUtils
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

class SnapshotCreateBuilder(repository: String, name: String, snapIndexList: Seq[String], wait: Boolean = false)
  extends AbstractMultiTypeActionBuilder[SnapshotCreateAction, SnapshotCreateBuilder] {
  var indexList = snapIndexList
  val snapshotRepository = repository
  val snapName = name
  val waitForCompletion = wait

  override def build: SnapshotCreateAction = new SnapshotCreateAction(this)
}

class SnapshotCreateAction(builder: SnapshotCreateBuilder) extends GenericResultAbstractAction(builder) {
  val indexList = builder.indexList
  val repository = builder.snapshotRepository
  val snapName = builder.snapName
  setURI(buildURI)

  override def getRestMethodName: String = "PUT"

  override def getData(gson: Gson): String = {
    if (indexList.size == 0) {
      // scalastyle:off null
      null // JEST needs a null, so we have to use it
      // scalastyle:on null
    } else {
      val payload : Map[String, String] =  Map("indices" -> indexList.mkString(","))
      JsonUtils.toJson(payload)
    }
  }

  protected override def buildURI: String = s"_snapshot/$repository/$snapName?wait_for_completion=${builder.waitForCompletion}"
}
