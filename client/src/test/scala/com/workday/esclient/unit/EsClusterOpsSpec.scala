package com.workday.esclient.unit

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import com.workday.esclient._
import com.workday.esclient.actions._
import com.workday.esclient.unit.EsClientSpec.EsClientWithMockedEs
import io.searchbox.client.JestClient
import io.searchbox.cluster.Health
import io.searchbox.core.Cat
import io.searchbox.indices.ClearCache
import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify}

class EsClusterOpsSpec extends EsClientSpec {
  val mapper= new ObjectMapper()

  behavior of "#clusterHealth"
  it should "execute a Health action and pass on the response" in {
    val mockJestClient = mock[JestClient]
    val response = HealthResponse("", "", false, 0, 0, 0, 0, 0, 0, 0)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.clusterHealth.get shouldEqual response
    verify(mockJestClient).execute(any(classOf[Health]))
  }

  behavior of "#worstIndexHealth"
  it should "execute a IndexHealth action and pass on the response" in {
    val mockJestClient = mock[JestClient]
    val response = HealthResponse("", "red", false, 0, 0, 0, 0, 0, 0, 0)

    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    esClient.worstIndexHealth(Seq("dev@super", "dev@super@stuff")) shouldEqual ("red", false)
    verify(mockJestClient).execute(any(classOf[Cat]))
  }

  behavior of "#catShards"
  it should "return the health of all the shards associated with an Index" in {
    val mockJestClient = mock[JestClient]

    val response = Seq(Map("index" -> "test_index", "shard" -> "0", "state" -> "UNASSIGNED", "node" -> "node_1", "prirep" -> "p", "store" -> "100"),
      Map("index" -> "test_index", "shard" -> "1", "state" -> "UNASSIGNED", "node" -> "node_1", "prirep" -> "p", "store" -> "100"))

    val esClient = new EsClientWithMockedEs(mockJestClient).whenCatShards(JsonUtils.toJson(response))
    val expectedShards = Seq(ShardInfo(index="test_index", shard=0, state="UNASSIGNED", node="node_1", docs=0, prirep="p", store=100),
      ShardInfo(index="test_index", shard=1, state="UNASSIGNED", node="node_1", docs=0, prirep="p", store=100))
    esClient.catShards("test_index").get shouldEqual expectedShards
    verify(mockJestClient).execute(any(classOf[CatAction]))
  }

  behavior of "#allocateShards"
  it should "allocate the shards and pass on the response" in {
    val mockJestClient = mock[JestClient]
    val response = RerouteAcknowledgment("true")

    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))
    esClient.allocateShards(Nil).get shouldEqual response
    verify(mockJestClient).execute(any(classOf[Reroute]))
  }

  behavior of "#availableNodes"
  it should "return all online nodes in Elasticsearch" in {
    val mockJestClient = mock[JestClient]
    val response = Seq(Map("name" -> "node2", "node.role" -> "d"),
      Map("name" -> "node4", "node.role" -> "d"))

    val esClient = new EsClientWithMockedEs(mockJestClient).whenCatAction(JsonUtils.toJson(response))
    esClient.availableNodes.get shouldEqual Seq(NodeInfo("node2", NodeInfo.DATA_NODE), NodeInfo("node4", NodeInfo.DATA_NODE))
    verify(mockJestClient).execute(any(classOf[CatAction]))
  }

  behavior of "#catIndex"
  it should "return the index status of the requested index" in {
    val mockJestClient = mock[JestClient]
    val indexStatus = Map("health" -> "green",
      "status"-> "open",
      "index"-> "index1",
      "pri"-> "1",
      "rep"-> "0",
      "docs.count"-> "123",
      "docs.deleted"-> "0",
      "store.size:"-> "1gb",
      "pri.store.size"-> "1gb")
    val response = Seq(indexStatus)

    val esClient = new EsClientWithMockedEs(mockJestClient).whenCatAction(JsonUtils.toJson(response))
    val result : Seq[IndexInfo] = esClient.catIndex("index1").get
    result.length shouldEqual 1
    result(0).index shouldEqual "index1"
    result(0).status shouldEqual "open"
    verify(mockJestClient).execute(any(classOf[CatAction]))
  }

  behavior of "#catAllIndices"
  it should "return all indices in ES" in {
    val mockJestClient = mock[JestClient]
    val indexOneStatus = Map("health" -> "green",
      "status"-> "open",
      "index"-> "index1",
      "pri"-> "1",
      "rep"-> "0",
      "docs.count"-> "123",
      "docs.deleted"-> "0",
      "store.size:"-> "1gb",
      "pri.store.size"-> "1gb")
    val indexTwoStatus = Map("health" -> "red",
      "status"-> "open",
      "index"-> "index2",
      "pri"-> "1",
      "rep"-> "0",
      "docs.count"-> "456",
      "docs.deleted"-> "0",
      "store.size:"-> "2gb",
      "pri.store.size"-> "2gb")
    val response = Seq(indexOneStatus, indexTwoStatus)

    val esClient = new EsClientWithMockedEs(mockJestClient).whenCatIndices(JsonUtils.toJson(response))
    val result : Seq[IndexInfo] = esClient.catAllIndices.get
    result.length shouldEqual 2
    result(0).index shouldEqual "index1"
    result(1).index shouldEqual "index2"
    verify(mockJestClient).execute(any(classOf[CatAction]))
  }

  behavior of "#clearCacheKeys"
  it should "clear the cache" in {
    val mockJestClient = mock[JestClient]
    val response = ClearCacheResponse(ClearCacheShards(1, 1, 0))
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))
    esClient.clearCacheKeys(Nil).get shouldEqual response
    esClient.clearCacheKeys(Seq("cachekey1", "cachekey2")).get shouldEqual response
    verify(mockJestClient, times(2)).execute(any(classOf[ClearCache]))
  }

  behavior of "#buildCatIndices"
  it should "create a jest Cat for Indices Health" in {
    val esClient = new EsClient(mock[JestClient])
    val catIndices = esClient.buildCatIndices()

    catIndices.getRestMethodName shouldEqual "GET"
  }

  behavior of "#buildCatShards"
  it should "create a jest Cat for Shards health" in {
    val esClient = new EsClient(mock[JestClient])
    val catShards = esClient.buildCatShards()

    catShards.getRestMethodName shouldEqual "GET"
    catShards.buildURI shouldEqual "_cat/shards/?format=json&bytes=b"
  }

  behavior of "#buildCatAction"
  it should "create a jest Cat Action for Shards Info" in {
    val esClient = new EsClient(mock[JestClient])
    val indexName = "testIndex"
    val catShardsAction = esClient.buildCatAction(CatAction.CAT_SHARDS, indexName)

    catShardsAction.operationPath shouldEqual CatAction.CAT_SHARDS
    catShardsAction.uriSuffix shouldEqual indexName
  }

  it should "create a jest Cat Action for Nodes Info" in {
    val esClient = new EsClient(mock[JestClient])
    val catNodesAction = esClient.buildCatAction(CatAction.CAT_NODES)

    catNodesAction.operationPath shouldEqual CatAction.CAT_NODES
  }

  // Because NodeStat needs to take nested JSON as parameters, we need to construct those JSON objects to use
  val jvmNode = mapper.readTree("""{"mem":{"heap_used_percent":50,"heap_used_in_bytes":1024,"heap_max_in_bytes":2048}}""")
  val fsNode = mapper.readTree("""{"total":{"total_in_bytes":2048,"available_in_bytes":512}}""")
  val attributesNode = mapper.readTree("""{"esx_group":"even"}""")
  val testNode = NodeStat(host="host", name="node1", jvmNode, fsNode, attributesNode)

  behavior of "#catNodesStats"
  it should "get nodes statistics from ES" in {
    val mockJestClient = mock[JestClient]

    val response = AllNodesStat(nodes=Map("id1" -> testNode))

    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))
    esClient.catNodesStats.get.nodes shouldEqual Map("id1" -> testNode)
  }

  behavior of "#NodeStat"
  it should "calculate occupied disk space" in {
    testNode.diskUsedInBytes shouldEqual Some(1536)
  }

  it should "correctly parse nested json properties" in {
    testNode.jvmHeapUsedPercent shouldEqual 50
    testNode.jvmHeapUsedBytes shouldEqual 1024
    testNode.jvmHeapMaxBytes shouldEqual 2048

    testNode.diskTotalInBytes shouldEqual Some(2048)
    testNode.diskAvailableInBytes shouldEqual Some(512)
    //TODO: import this package
    //testNode.esxGroup shouldEqual Some("even")
  }

  it should "correctly deal with missing properties" in {
    val fsNodeEmpty = mapper.readTree("""{"total":{}}""")
    val attributesNodeEmpty = mapper.readTree("""{}""")
    val testNode = NodeStat(host="host", name="node1", jvmNode, fsNodeEmpty, attributesNodeEmpty)

    testNode.diskTotalInBytes shouldEqual None
    testNode.diskAvailableInBytes shouldEqual None
    //TODO: import this package
    //testNode.esxGroup shouldEqual None

    testNode.diskUsedInBytes shouldEqual None
  }

  it should "correctly implement toString" in {
    val testNode = NodeStat(host="host", name="node1", jvmNode, fsNode, attributesNode)
    val expectedString = "NodeStat(host,node1,50,1024,2048,Some(2048),Some(512),Some(even))"

    testNode.toString shouldEqual expectedString
  }

  behavior of "#clusterState"

  // Because NodeStat needs to take nested JSON as parameters, we need to construct those JSON objects to use
  val nodesNode = mapper.readTree("""{"node0": {
                                        "name": "esd-pdxeng-12-syman",
                                        "transport_address": "inet[/10.52.87.164:9300]",
                                        "attributes": {
                                          "esx_group": "even",
                                          "master": "false"
                                        }
                                      }}""")
  val metadataNode = mapper.readTree("""{
                                           "templates": {},
                                           "indices": {
                                             "4882379a-c876-4f0b-a802-458c02c945bf@docs": {
                                               "state": "open"}}}""")

  val blocksNode = mapper.readTree("""{}""")
  val routingTableStringIndex0 = """"blogs0": {
                                    "shards": {
                                             "0": [
                                                    {
                                                      "state": "INITIALIZING",
                                                      "primary": false,
                                                      "node": "node0",
                                                      "relocating_node": null,
                                                      "shard": 0,
                                                      "index": "blogs0",
                                                      "unassigned_info":
                                                      {
                                                        "reason":"ALLOCATION FAILED",
                                                        "at": "2016-10-06T20:21:08.653Z",
                                                        "details":"[MISSING_TENANT_KEY] Shard allocation exception."
                                                      }
                                                     },
                                                     {
                                                      "state": "UNASSIGNED",
                                                      "primary": true,
                                                      "node": "node1",
                                                      "relocating_node": null,
                                                      "shard": 0,
                                                      "index": "blogs0",
                                                      "unassigned_info":
                                                      {
                                                        "reason":"ALLOCATION FAILED",
                                                        "details":"[MISSING_TENANT_KEY] Shard allocation exception."
                                                      }
                                                     }
                                                    ],
                                               "1": [
                                                      {
                                                        "state": "STARTED",
                                                        "primary": true,
                                                        "node": "node0",
                                                        "relocating_node": null,
                                                        "shard": 1,
                                                        "index": "blogs0"
                                                      }
                                                    ],
                                               "2": [
                                                      {
                                                        "state": "INITIALIZING",
                                                        "primary": false,
                                                        "node": "node0",
                                                        "relocating_node": null,
                                                        "shard": 2,
                                                        "index": "blogs0",
                                                        "unassigned_info":
                                                        {
                                                          "reason": "ALLOCATION FAILED",
                                                          "details": "[MISSING_TENANT_KEY][HMAC_COLLISION] ENCRYPTED STORE(AES READER): Invalid HMAC value"
                                                        }
                                                      },
                                                      {
                                                        "state": "UNASSIGNED",
                                                        "primary": true,
                                                        "node": "node1",
                                                        "relocating_node": null,
                                                        "shard": 2,
                                                        "index": "blogs0",
                                                        "unassigned_info":
                                                        {
                                                          "reason": "ALLOCATION FAILED",
                                                          "details": "[MISSING_TENANT_KEY][HMAC_COLLISION] ENCRYPTED STORE(AES READER): Invalid HMAC value"
                                                        }
                                                      }
                                                    ]
                                                  }}"""
  val routingTableClusterNode = mapper.readTree(s"""{"indices": {${routingTableStringIndex0}}}""")
  val clusterName = "syman"
  val version = 334
  val masterNode = "node1"
  val clusterStateResponse = ClusterStateResponse(clusterName, Some(version), Some(masterNode), Some(nodesNode), Some(blocksNode), Some(routingTableClusterNode), Some(metadataNode))

  it should "execute a ClusterState action and pass on the response for entire cluster state." in {
    val mockJestClient = mock[JestClient]
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(clusterStateResponse))

    val actualClusterState = esClient.clusterState().get

    actualClusterState.clusterName shouldEqual clusterName
    actualClusterState.version shouldEqual Some(version)
    actualClusterState.masterNode shouldEqual Some(masterNode)
    actualClusterState.blocks shouldEqual Some(blocksNode)
    actualClusterState.routingTable shouldEqual Some(routingTableClusterNode)
    actualClusterState.metadata shouldEqual Some(metadataNode)
    actualClusterState.nodes shouldEqual Some(nodesNode)
  }

  it should "execute a ClusterState action and pass on the response for only routing table part of cluster state when routingTable parameter is set to true." in {
    val mockJestClient = mock[JestClient]
    val routingTableIndex0 = mapper.readTree(s"""{"indices": {${routingTableStringIndex0}}}""")
    val routingTableClusterStateResponse = ClusterStateResponse(clusterName, None, None, None, None, Some(routingTableIndex0), None)
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(routingTableClusterStateResponse))

    val actualClusterState = esClient.clusterState(withRoutingTable = true).get

    actualClusterState.clusterName shouldEqual clusterName
    actualClusterState.version shouldEqual None
    actualClusterState.masterNode shouldEqual None
    actualClusterState.blocks shouldEqual None
    actualClusterState.routingTable shouldEqual Some(routingTableIndex0)
    actualClusterState.metadata shouldEqual None
    actualClusterState.nodes shouldEqual None
  }

  behavior of "#ClusterStateResponse.getShards"
  it should "return a Sequence of ClusterStateShardInfo objects from the routing table." in {
    val routingTableIndex0 = mapper.readTree(s"""{"indices": {${routingTableStringIndex0}}}""")
    val routingTableClusterStateResponse = ClusterStateResponse(clusterName, None, None, None, None, Some(routingTableIndex0), None)
    val expectedResult = Seq(
                              ClusterStateShardInfo("INITIALIZING", false, Some("node0"), None, 0, "blogs0", Some(UnassignedInfo(Some("ALLOCATION FAILED"),
                                                                                                                                 Some("2016-10-06T20:21:08.653Z"),
                                                                                                                                 Some("[MISSING_TENANT_KEY] Shard allocation exception.")))),
                              ClusterStateShardInfo("UNASSIGNED", true, Some("node1"), None, 0, "blogs0", Some(UnassignedInfo(Some("ALLOCATION FAILED"),
                                                                                                                                 None,
                                                                                                                                 Some("[MISSING_TENANT_KEY] Shard allocation exception.")))),
                              ClusterStateShardInfo("STARTED", true, Some("node0"), None, 1, "blogs0", None),
                              ClusterStateShardInfo("INITIALIZING", false, Some("node0"), None, 2, "blogs0", Some(UnassignedInfo(Some("ALLOCATION FAILED"),
                                None,
                                Some("[MISSING_TENANT_KEY][HMAC_COLLISION] ENCRYPTED STORE(AES READER): Invalid HMAC value")))),
                              ClusterStateShardInfo("UNASSIGNED", true, Some("node1"), None, 2, "blogs0", Some(UnassignedInfo(Some("ALLOCATION FAILED"),
                                None,
                                Some("[MISSING_TENANT_KEY][HMAC_COLLISION] ENCRYPTED STORE(AES READER): Invalid HMAC value"))))
                        )

    routingTableClusterStateResponse.getShards shouldEqual expectedResult
  }

  behavior of "#updateClusterSettings"
  it should "execute a ClusterSettings action and pass on the response" in {
    val cs = new ClusterSettingsBuilder(transient = Map(), persistent = Map()).build
    cs.getRestMethodName shouldEqual "PUT"
    cs.getData(new Gson()) shouldEqual "{\"persistent\":{},\"transient\":{}}"

    val mockJestClient = mock[JestClient]
    val response = ClusterSettingsResponse(transient = Map("foo" -> "bar"), persistent = Map("bar" -> "baz"))
    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))

    val transient = new java.util.HashMap[String, String]()
    transient.put("foo", "bar")
    val persistent = new java.util.HashMap[String, String]()
    persistent.put("bar", "baz")

    esClient.updateClusterSettings(transient, persistent).get shouldEqual response
    verify(mockJestClient).execute(any(classOf[ClusterSettings]))
  }

  behavior of "#clusterSettings"
  it should "get current settings from ES" in {
    val clusterSettingsListAction = new ClusterSettingsListBuilder().build
    clusterSettingsListAction.getRestMethodName shouldEqual "GET"

    val mockJestClient = mock[JestClient]

    val response = ClusterSettingsResponse(transient = Map("foo" -> Map("bar" -> "baz")), persistent = Map("hello" -> "world"))

    val esClient = new EsClientWithMockedEs(mockJestClient).whenAny(JsonUtils.toJson(response))
    val clusterSettingsResponse = esClient.clusterSettings
    clusterSettingsResponse.get.transient shouldEqual Map("foo" -> Map("bar" -> "baz"))
    clusterSettingsResponse.get.persistent shouldEqual Map("hello" -> "world")
    verify(mockJestClient).execute(any(classOf[ClusterSettingsListAction]))
  }

}
