package com.workday.esclient

import com.fasterxml.jackson.databind.JsonNode
import com.workday.esclient.actions._

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
  * Elasticsearch Snapshot APIs
  */
trait EsSnapshots extends JestUtils {

  /**
    * Take a snapshot of a set of indices
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
    * Delete a snapshot
    */
  def snapshotDelete(repositoryName: String, snapshotName: String): EsResult[Acknowledgement] = {
    val deleteAction = new SnapshotDeleteBuilder(repositoryName, snapshotName).build
    val jestResult = jest.execute(deleteAction)
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Restore from a snapshot. The repository must be closed or this will fail
    */
  def snapshotRestore(repositoryName: String, snapshotName: String, waitForCompletion: Boolean = false): EsResult[AcceptResponse] = {
    val restoreAction = new SnapshotRestoreBuilder(repositoryName, snapshotName, waitForCompletion).build
    val jestResult = jest.execute(restoreAction)
    toEsResult[AcceptResponse](jestResult)
  }

  /**
    * Get the list of all snapshots defined for a specific repository
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
    * Create a repository with the specified settings. Settings are the payload as defined here:
    * https://www.elastic.co/guide/en/elasticsearch/reference/1.4/modules-snapshots.html#_repositories
    */
  def repositoryCreate(repositoryName: String, repositorySettings: String): EsResult[Acknowledgement] = {
    val createRepositoryAction = new RepositoryCreateBuilder(repositoryName, repositorySettings).build
    val jestResult = jest.execute(createRepositoryAction)
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Delete a repository
    */
  def repositoryDelete(repositoryName: String): EsResult[Acknowledgement] = {
    val deleteAction = new RepositoryDeleteBuilder(repositoryName).build
    val jestResult = jest.execute(deleteAction)
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Get the list of all repositories defined in the ES cluster
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

case class SnapshotResponse(
  snapshot: Option[JsonNode]
)

case class AcceptResponse(
  accepted: Boolean
)
