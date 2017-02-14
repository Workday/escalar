/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient

import com.fasterxml.jackson.databind.JsonNode
import com.workday.esclient.actions._

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
  * Trait wrapping Elasticsearch Snapshot APIs.
  */
trait EsSnapshots extends JestUtils {

  /**
    * Returns a snapshot of a set of indices.
    * @param repositoryName String name of target repository
    * @param snapshotName String name of snapshot
    * @param indices Sequence of String indices
    * @param waitForCompletion Boolean whether to wait for snapshot response; Defaults to false
    * @return EsResult of snapshot or accepted response
    */
  def snapshotCreate(repositoryName: String, snapshotName: String, indices: Seq[String], waitForCompletion: Boolean = false): EsResult[Any] = {
    val putAction = new SnapshotCreateBuilder(repositoryName, snapshotName, indices, waitForCompletion).build
    val jestResult = jest.execute(putAction)
    if(waitForCompletion)
      toEsResult[SnapshotResponse](jestResult)  // TODO: any way to collapse this and put the conditional inside the type mapping?
    else
      toEsResult[AcceptResponse](jestResult)
  }

  /**
    * Deletes a snapshot from given repository and returns acknowledgment.
    * @param repositoryName String name of target repository
    * @param snapshotName String name of snapshot
    * @return EsResult containing an Elasticsearch acknowledgment
    */
  def snapshotDelete(repositoryName: String, snapshotName: String): EsResult[Acknowledgement] = {
    val deleteAction = new SnapshotDeleteBuilder(repositoryName, snapshotName).build
    val jestResult = jest.execute(deleteAction)
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Restores from a snapshot and returns an accepted response from Elasticsearch.
    * Repository must be closed or this will fail.
    * @param repositoryName String name of target repository
    * @param snapshotName String name of snapshot
    * @param waitForCompletion Boolean whether to wait for snapshot response; Defaults to false
    * @return EsResult containing an Elasticsearch accepted response
    */
  def snapshotRestore(repositoryName: String, snapshotName: String, waitForCompletion: Boolean = false): EsResult[AcceptResponse] = {
    val restoreAction = new SnapshotRestoreBuilder(repositoryName, snapshotName, waitForCompletion).build
    val jestResult = jest.execute(restoreAction)
    toEsResult[AcceptResponse](jestResult)
  }

  /**
    * Returns list of all snapshots defined for a given repository.
    * Maps Elasticsearch response JSON objects to a sequence of JSON objects of snapshots and their indices then
    * wraps this sequence in a case class.
    * @param repositoryName String name of target repository
    * @return EsResult containing an list of snapshots as [[com.fasterxml.jackson.databind.JsonNode]]
    */
  def snapshotList(repositoryName: String): EsResult[ListSnapshotResponse] = {
    val listSnapshotAction = new SnapshotListBuilder(repositoryName).build
    val jestResult = jest.execute(listSnapshotAction)

    handleJestResult(jestResult) { successfulJestResult =>
      val json = successfulJestResult.getJsonObject

      val snapshotSeq: Seq[SnapshotDefinition] = json.get("snapshots").getAsJsonArray.iterator().asScala.toSeq.map(snapshot => {
        val s = snapshot.getAsJsonObject
        val indexList = s.get("indices").getAsJsonArray.iterator().asScala.toSeq.map(indexObj =>
          indexObj.getAsString
        )
        SnapshotDefinition(s.get("snapshot").getAsString, indexList, s.get("state").getAsString)
      })
      ListSnapshotResponse(snapshotSeq)
    }
  }

  /**
    * Creates a repository with the specified settings and returns an Elasticsearch acknowledgment.
    * Settings are the payload as defined here:
    * https://www.elastic.co/guide/en/elasticsearch/reference/1.4/modules-snapshots.html#_repositories
    * @param repositoryName String name of target repository
    * @param repositorySettings String of repository settings
    * @return EsResult containing an Elasticsearch acknowledgment
    */
  def repositoryCreate(repositoryName: String, repositorySettings: String): EsResult[Acknowledgement] = {
    val createRepositoryAction = new RepositoryCreateBuilder(repositoryName, repositorySettings).build
    val jestResult = jest.execute(createRepositoryAction)
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Deletes a repository and returns an Elasticsearch acknowledgment.
    * @param repositoryName String name of target repository
    * @return EsResult containing an Elasticsearch acknowledgment
    */
  def repositoryDelete(repositoryName: String): EsResult[Acknowledgement] = {
    val deleteAction = new RepositoryDeleteBuilder(repositoryName).build
    val jestResult = jest.execute(deleteAction)
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Returns the list of all repositories defined in the ES cluster.
    * @return EsResult containing a sequence of repository definitions
    */
  def repositoryList(): EsResult[ListRepositoryResponse] = {
    val listRepositoriesAction = new RepositoryListBuilder().build
    val jestResult = jest.execute(listRepositoriesAction)

    handleJestResult(jestResult) { successfulJestResult =>
      val json = successfulJestResult.getJsonObject
      val repoMap = JsonUtils.fromJson[Map[String, Map[String, Any]]](json.toString)
      val reposSeq : Seq[RepositoryDefinition] = repoMap.map( repository => {
        val name = repository._1
        val repoType = repository._2("type").asInstanceOf[String]
        val repoSettings = repository._2("settings").asInstanceOf[Map[String, String]]
        RepositoryDefinition(name, repoType, repoSettings)
      }).toSeq

      ListRepositoryResponse(reposSeq)
    }
  }
}

/**
  * Case class for an Elasticsearch Snapshot response.
  * @param snapshot Ssnapshot as option of [[com.fasterxml.jackson.databind.JsonNode]]
  */
case class SnapshotResponse(
  snapshot: Option[JsonNode]
)

/**
  * Case class for an Elasticsearch accept response.
  * @param accepted Boolean whether the request was accepted
  */
case class AcceptResponse(
  accepted: Boolean
)
