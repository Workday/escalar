package com.workday.esclient.unit

import com.workday.esclient.actions._
import com.workday.esclient.unit.EsClientSpec.EsClientWithMockedEs
import com.workday.esclient.{AcceptResponse, Acknowledgement, JsonUtils, SnapshotResponse}
import io.searchbox.client.JestClient
import org.mockito.Matchers.any
import org.mockito.Mockito.verify

class EsSnapshotsSpec extends EsClientSpec {

  val TEST_REPOSITORY = "test_repo"
  val TEST_SNAPSHOT = "test_snap"

  behavior of "#snapshotCreate"
  it should "create a snapshot without waiting for completion" in {
    val mockJestClient = mock[JestClient]
    val acceptResponse = AcceptResponse(true)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(acceptResponse))

    esClient.snapshotCreate(TEST_REPOSITORY, TEST_SNAPSHOT, Seq("")).get shouldEqual acceptResponse
    verify(mockJestClient).execute(any(classOf[SnapshotCreateAction]))
  }
  it should "create a snapshot and wait for completion" in {
    val mockJestClient = mock[JestClient]
    val snapshotResponse = SnapshotResponse(None)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(snapshotResponse))

    esClient.snapshotCreate(TEST_REPOSITORY, TEST_SNAPSHOT, Seq(""), true).get shouldBe a [SnapshotResponse]
    verify(mockJestClient).execute(any(classOf[SnapshotCreateAction]))
  }

  behavior of "#snapshotDelete"
  it should "delete a snapshot" in {
    val mockJestClient = mock[JestClient]
    val response = Acknowledgement(true)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.snapshotDelete(TEST_REPOSITORY, TEST_SNAPSHOT).get shouldEqual response
    verify(mockJestClient).execute(any(classOf[SnapshotDeleteAction]))
  }

  behavior of "#snapshotRestore"
  it should "restore a snapshot" in {
    val mockJestClient = mock[JestClient]
    val response = AcceptResponse(true)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.snapshotRestore(TEST_REPOSITORY, TEST_SNAPSHOT).get shouldEqual response
    verify(mockJestClient).execute(any(classOf[SnapshotRestoreAction]))
  }

  behavior of "#snapshotList"
  it should "list the snapshots" in {
    val mockJestClient = mock[JestClient]

    val response = Map("snapshots" -> Seq(
      Map("snapshot"->"snapname1", "indices"->Seq("index1"), "state" -> "SUCCESS"),
      Map("snapshot"->"snapname2", "indices"->Seq("index2", "index3"), "state" -> "FAILED")
    ))

    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    val actualResponse = esClient.snapshotList(TEST_REPOSITORY).get
    actualResponse.snapshots.size shouldEqual 2
    actualResponse.snapshots(0).snapshotName shouldEqual "snapname1"
    actualResponse.snapshots(0).state shouldEqual "SUCCESS"
    actualResponse.snapshots(0).indices.size shouldEqual 1
    actualResponse.snapshots(0).indices(0) shouldEqual "index1"

    actualResponse.snapshots(1).snapshotName shouldEqual "snapname2"
    actualResponse.snapshots(1).state shouldEqual "FAILED"
    actualResponse.snapshots(1).indices.size shouldEqual 2
    actualResponse.snapshots(1).indices(0) shouldEqual "index2"
    actualResponse.snapshots(1).indices(1) shouldEqual "index3"

    verify(mockJestClient).execute(any(classOf[SnapshotListAction]))
  }

  behavior of "#repositoryCreate"
  it should "create a repository" in {
    val mockJestClient = mock[JestClient]
    val response = Acknowledgement(true)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.repositoryCreate(TEST_REPOSITORY, "foo").get shouldEqual response
    verify(mockJestClient).execute(any(classOf[RepositoryCreateAction]))
  }

  behavior of "#repositoryDelete"
  it should "delete a repository" in {
    val mockJestClient = mock[JestClient]
    val response = Acknowledgement(true)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.repositoryDelete(TEST_REPOSITORY).get shouldEqual response
    verify(mockJestClient).execute(any(classOf[RepositoryDeleteAction]))
  }

  behavior of "#repositoryList"
  it should "list the repositories" in {
    val mockJestClient = mock[JestClient]

    val response = Map("repository1" -> Map("type" -> "fs", "settings" -> Map("location"-> "/tmp/foo")))
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    val actualResponse = esClient.repositoryList().get
    actualResponse.repositories.size shouldEqual 1
    actualResponse.repositories(0).repoName shouldEqual "repository1"
    actualResponse.repositories(0).repoType shouldEqual "fs"
    val settings = actualResponse.repositories(0).settings.asInstanceOf[Map[String, String]]
    settings("location") shouldEqual "/tmp/foo"

    verify(mockJestClient).execute(any(classOf[RepositoryListAction]))
  }
}
