/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import com.google.gson.Gson
import com.workday.esclient.JsonUtils
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

/**
  * Build class for [[com.workday.esclient.actions.SnapshotCreateAction]].
  * @param repository String repository name.
  * @param name String snapshot name.
  * @param snapIndexList Sequence of indices in snapshot.
  * @param wait Boolean whether to wait for completion of action.
  */
class SnapshotCreateBuilder(repository: String, name: String, snapIndexList: Seq[String], wait: Boolean = false)
  extends AbstractMultiTypeActionBuilder[SnapshotCreateAction, SnapshotCreateBuilder] {
  var indexList = snapIndexList
  val snapshotRepository = repository
  val snapName = name
  val waitForCompletion = wait

  /**
    * Builds [[com.workday.esclient.actions.SnapshotCreateAction]].
    * @return [[com.workday.esclient.actions.SnapshotCreateAction]].
    */
  override def build: SnapshotCreateAction = new SnapshotCreateAction(this)
}

/**
  * Action class for creating snapshots using the Elasticsearch Snapshot API.
  * @param builder [[com.workday.esclient.actions.SnapshotCreateBuilder]].
  */
class SnapshotCreateAction(builder: SnapshotCreateBuilder) extends GenericResultAbstractAction(builder) {
  val indexList = builder.indexList
  val repository = builder.snapshotRepository
  val snapName = builder.snapName
  setURI(buildURI)

  /**
    * Gets REST method name.
    * @return String "PUT".
    */
  override def getRestMethodName: String = "PUT"

  /**
    * Gets snapshot data.
    * @param gson [[com.google.gson.Gson]] JSON object.
    * @return String JSON of indices on snapshot.
    */
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

  /**
    * Builds URI for hitting Snapshot API.
    * @return String URI.
    */
  protected override def buildURI: String = s"_snapshot/$repository/$snapName?wait_for_completion=${builder.waitForCompletion}"
}
