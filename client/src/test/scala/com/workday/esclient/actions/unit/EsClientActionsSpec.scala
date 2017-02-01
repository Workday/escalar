package com.workday.esclient.actions.unit

import com.google.gson.{Gson, JsonObject}
import com.workday.esclient.JsonUtils
import com.workday.esclient.actions._
import io.searchbox.core.CatResult

class EsClientActionsSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll
  with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar {

  behavior of "#CatAction"
  it should "have a REST method type of GET" in {
    val catAction: CatAction = new CatBuilder("").build
    catAction.getRestMethodName shouldEqual "GET"
  }

  it should "have a top key named result" in {
    val catAction: CatAction = new CatBuilder("").build
    catAction.getPathToResult shouldEqual "result"
  }

  it should "parse a Sequence of Response string to a JSON object where the top key is named result." in {
    val catAction: CatAction = new CatBuilder("").build
    val jsonResponse = Seq(Map("name" -> "node2", "node.role" -> "d"),
                          Map("name" -> "node4", "node.role" -> "d"))
    catAction.parseResponseBody(JsonUtils.toJson(jsonResponse)).toString() shouldEqual
                                        JsonUtils.toJson(Map("result" -> jsonResponse))
  }

  it should "parse error JSON to a JSON object." in {
    val catAction: CatAction = new CatBuilder("").build
    val jsonResponse = Map("error" -> "err", "status" -> 500)
    val expectedJson = "{\"error\":\"err\",\"status\":500}"
    catAction.parseResponseBody(JsonUtils.toJson(jsonResponse)).toString() shouldEqual expectedJson
  }

  it should "return empty result when the user passes in null as the response body" in {
    val catAction: CatAction = new CatBuilder("").build
    val emptyJsonObject = new JsonObject()

    catAction.parseResponseBody(null).toString shouldEqual emptyJsonObject.toString
  }

  it should "return empty result when the user passes in a string with white spaces as the response body" in {
    val catAction: CatAction = new CatBuilder("").build
    val emptyJsonObject = new JsonObject()

    catAction.parseResponseBody("  \n").toString shouldEqual emptyJsonObject.toString
  }

  it should "return a CatResult with correct response and status code etc properties" in {
    val responseString = JsonUtils.toJson(Seq(Map("acknowledged" -> "true")))
    val responseCode = 200

    val expectedCatResult: CatResult = new CatResult(new Gson())
    expectedCatResult.setJsonString(responseString)
    expectedCatResult.setResponseCode(responseCode)
    expectedCatResult.setSucceeded(true)
    expectedCatResult.setPathToResult("result")

    val catAction: CatAction = new CatBuilder("shard").build
    val actualCatResult = catAction.createNewElasticSearchResult(responseString, 200, "", new Gson())

    actualCatResult.getJsonString shouldEqual expectedCatResult.getJsonString
    actualCatResult.getResponseCode shouldEqual expectedCatResult.getResponseCode
    actualCatResult.isSucceeded shouldEqual expectedCatResult.isSucceeded
    actualCatResult.getPathToResult shouldEqual expectedCatResult.getPathToResult
  }

  behavior of "#NodeInfo#isDataNode"
  it should "return true if the node is a data node." in {
    val node = NodeInfo("Thor", "d")

    node.isDataNode shouldEqual true
  }

  it should "return false if the node is not a data node." in {
    val node = NodeInfo("Thor", "c")

    node.isDataNode shouldEqual false
  }

  behavior of "#IndexHealthAction"
  it should "correctly build the URI for an index health action" in {
    val action: IndexHealthAction = new IndexHealthBuilder(Seq("dev@super", "dev@super@system_accounts"), "15ms").build
    action.getURI shouldEqual "/_cluster/health/dev@super,dev@super@system_accounts?timeout=15ms"
  }

  behavior of "#RepositoryCreateAction"
  it should "have a REST method type of PUT" in {
    val action: RepositoryCreateAction = new RepositoryCreateBuilder("", "").build
    action.getRestMethodName shouldEqual "PUT"
  }

  it should "return the repository definition as the payload" in {
    val repoPayload = "test payload"
    val action: RepositoryCreateAction = new RepositoryCreateBuilder("", repoPayload).build
    action.getData(null) shouldEqual repoPayload
  }

  it should "return the url to do the create request" in {
    val action: RepositoryCreateAction = new RepositoryCreateBuilder("reponame", "").build
    action.getURI shouldEqual "_snapshot/reponame"
  }

  behavior of "#RepositoryDeleteAction"
  it should "have a REST method type of DELETE" in {
    val action: RepositoryDeleteAction = new RepositoryDeleteBuilder("reponame").build
    action.getRestMethodName shouldEqual "DELETE"
  }

  it should "return an empty payload" in {
    val action: RepositoryDeleteAction = new RepositoryDeleteBuilder("reponame").build
    action.getData(null) shouldEqual null
  }

  it should "return the url to do the repository delete" in {
    val action: RepositoryDeleteAction = new RepositoryDeleteBuilder("reponame").build
    action.getURI shouldEqual "_snapshot/reponame"
  }

  behavior of "#RepositoryListAction"
  it should "have a REST method type of GET" in {
    val action: RepositoryListAction = new RepositoryListBuilder().build
    action.getRestMethodName shouldEqual "GET"
  }

  it should "return an empty payload" in {
    val action: RepositoryListAction = new RepositoryListBuilder().build
    action.getData(null) shouldEqual null
  }

  it should "return the url to do the list request" in {
    val action: RepositoryListAction = new RepositoryListBuilder().build
    action.getURI shouldEqual "_snapshot/_all"
  }

  behavior of "#Reroute"
  it should "have a REST method type of POST" in {
    val reroute: Reroute = new RerouteBuilder(Seq()).build
    reroute.getRestMethodName shouldEqual "POST"
  }

  it should "have getData return an empty commands list if the payload is null" in {
    val reroute: Reroute = new RerouteBuilder(Seq()).build
    reroute.getData(null) shouldEqual JsonUtils.toJson(Map("commands" -> Seq()))
  }

  it should "have getData return commands List for shards reallocation if the payload for shards and destination nodes is provided" in {
    val indexName = "index1"
    val destNodeName = "Iron Man"
    val rerouteAllocate1 = RerouteAllocate(indexName, 0, "Iron Man")
    val rerouteAllocate2 = RerouteAllocate(indexName, 1, "Iron Man")
    val reallocateSeq = Seq(rerouteAllocate1, rerouteAllocate2)
    val reroute: Reroute = new RerouteBuilder(reallocateSeq).build

    val result = Map("commands" -> Seq(
      Map("allocate" ->
        Map("index" -> indexName,
            "shard" -> 0,
            "node" -> destNodeName,
            "allow_primary" -> "true")
      ),
      Map("allocate" ->
        Map("index" -> indexName,
             "shard" -> 1,
             "node" -> destNodeName,
             "allow_primary" -> "true"))
      )
    )
    reroute.getData(null) shouldEqual JsonUtils.toJson(result)
  }

  behavior of "#RerouteAllocate"
  it should "create a Map from the case class" in {
    val rerouteAllocate = RerouteAllocate("index", 0, "node")
    val expectedResult = Map("allocate" ->
      Map("index" -> "index", "shard" -> 0,
        "node" -> "node", "allow_primary" -> "true")
    )

    rerouteAllocate.toMap shouldEqual expectedResult
  }

  behavior of "#RerouteMove"
  it should "create a Map from the case class" in {
    val rerouteMove = RerouteMove("index", 0, "node1", "node2")
    val expectedResult = Map("move" ->
      Map("index" -> "index", "shard" -> 0,
        "from_node" -> "node1", "to_node" -> "node2")
    )

    rerouteMove.toMap shouldEqual expectedResult
  }

  behavior of "#SnapshotCreateAction"
  it should "have a REST method type of PUT" in {
    val action: SnapshotCreateAction = new SnapshotCreateBuilder("", "", Nil).build
    action.getRestMethodName shouldEqual "PUT"
  }

  it should "return an empty payload if no indices are specified" in {
    val action: SnapshotCreateAction = new SnapshotCreateBuilder("reponame", "snapshotname", Nil).build
    action.getData(null) shouldEqual null
  }

  it should "return a snapshot payload if indicies are specified" in {
    val action: SnapshotCreateAction = new SnapshotCreateBuilder("reponame", "snapshotname", Seq("index1", "index2")).build
    action.getData(null) shouldEqual JsonUtils.toJson(Map("indices" -> "index1,index2"))
  }

  it should "return the url to do the create request with wait_for_completion = false" in {
    val action: SnapshotCreateAction = new SnapshotCreateBuilder("reponame", "snapshotname", Nil).build
    action.getURI shouldEqual "_snapshot/reponame/snapshotname?wait_for_completion=false"
  }

  it should "return the url to do the create request with wait_for_completion = true" in {
    val action: SnapshotCreateAction = new SnapshotCreateBuilder("reponame", "snapshotname", Nil, true).build
    action.getURI shouldEqual "_snapshot/reponame/snapshotname?wait_for_completion=true"
  }

  behavior of "#SnapshotDeleteAction"
  it should "have a REST method type of DELETE" in {
    val action: SnapshotDeleteAction = new SnapshotDeleteBuilder("", "").build
    action.getRestMethodName shouldEqual "DELETE"
  }

  it should "return an empty payload" in {
    val action: SnapshotDeleteAction = new SnapshotDeleteBuilder("", "").build
    action.getData(null) shouldEqual null
  }

  it should "return the uri to do the delete request" in {
    val action: SnapshotDeleteAction = new SnapshotDeleteBuilder("reponame", "snapshotname").build
    action.getURI shouldEqual "_snapshot/reponame/snapshotname"
  }

  behavior of "#SnapshotListAction"
  it should "have a REST method type of GET" in {
    val action: SnapshotListAction = new SnapshotListBuilder("").build
    action.getRestMethodName shouldEqual "GET"
  }

  it should "return an empty payload" in {
    val action: SnapshotListAction = new SnapshotListBuilder("").build
    action.getData(null) shouldEqual null
  }

  it should "return the uri for the list action" in {
    val action: SnapshotListAction = new SnapshotListBuilder("reponame").build
    action.getURI shouldEqual "_snapshot/reponame/_all"
  }

  behavior of "#SnapshotRestoreAction"
  it should "have a REST method type of POST" in {
    val action: SnapshotRestoreAction = new SnapshotRestoreBuilder("", "").build
    action.getRestMethodName shouldEqual "POST"
  }

  it should "return an empty payload" in {
    val action: SnapshotRestoreAction = new SnapshotRestoreBuilder("", "").build
    action.getData(null) shouldEqual null
  }

  it should "return the uri for the restore action with wait_for_completion = false" in {
    val action: SnapshotRestoreAction = new SnapshotRestoreBuilder("reponame", "snapshotname").build
    action.getURI shouldEqual "_snapshot/reponame/snapshotname/_restore?wait_for_completion=false"
  }

  it should "return the uri for the restore action with wait_for_completion = true" in {
    val action: SnapshotRestoreAction = new SnapshotRestoreBuilder("reponame", "snapshotname", true).build
    action.getURI shouldEqual "_snapshot/reponame/snapshotname/_restore?wait_for_completion=true"
  }

  behavior of "#GetAliasByNameAction"
  it should "have a REST method type of GET" in {
    val action: GetAliasByNameAction = new GetAliasByNameBuilder(Nil).build
    action.getRestMethodName shouldEqual "GET"
  }

  it should "return an empty payload" in {
    val action: GetAliasByNameAction = new GetAliasByNameBuilder(Nil).build
    action.getData(null) shouldEqual null
  }

  it should "return the uri to do the get request for one alias" in {
    val action: GetAliasByNameAction = new GetAliasByNameBuilder(Seq("alias1")).build
    action.getURI shouldEqual "_alias/alias1?format=json&bytes=b"
  }

  it should "return the uri to do the get request for multiple aliases" in {
    val action: GetAliasByNameAction = new GetAliasByNameBuilder(Seq("alias1", "alias2")).build
    action.getURI shouldEqual "_alias/alias1,alias2?format=json&bytes=b"
  }
}
