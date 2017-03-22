package com.workday.esclient.unit

import com.google.gson.Gson
import com.workday.esclient._
import io.searchbox.client.JestClient
import io.searchbox.indices.aliases.GetAliases
import org.mockito.Matchers._
import org.mockito.Mockito._

class EsAliasesSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll
  with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar {
  val toAdd = Seq(
    AliasInfo("index1", "dev@test11", Some("1"), Some("1")),
    AliasInfo("index2", "dev@test12")
  )
  val toRemove = Seq(
    AliasInfo("index1", "dev@test13", Some("1"), Some("1"))
  )

  behavior of "#createAliases"
  it should "create aliases" in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val expectedResponse = EsResponse(Acknowledgement_1_7(true))
    doReturn(expectedResponse).when(esClient).modifyAliases(any(), any())

    esClient.createAliases(toAdd) shouldEqual expectedResponse
  }

  behavior of "#deleteAliases"
  it should "delete aliases" in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val expectedResponse = EsResponse(Acknowledgement_1_7(true))
    doReturn(expectedResponse).when(esClient).modifyAliases(any(), any())

    esClient.deleteAliases(toRemove) shouldEqual expectedResponse
  }

  behavior of "#modifyAliases"
  it should "create and delete aliases" in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val expectedResponse = EsResponse(Acknowledgement_1_7(true))
    doReturn(expectedResponse).when(esClient).toEsResult[Acknowledgement_1_7](any(), anyBoolean())(any[Manifest[Acknowledgement_1_7]]) // yay implicits...

    esClient.modifyAliases(toAdd, toRemove) shouldEqual expectedResponse
  }

  behavior of "#catAliases"
  it should "Get a list of all aliases on the ES cluster." in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val getAliasesResponse = EsResponse(Map("dev@oms" -> Seq(AliasInfo("dev@oms", "Alias_dev@oms", Some("1"), Some("1"))),
                                      "dev@super" -> Seq(AliasInfo("dev@super", "Alias_dev@super", Some("2"), Some("2"))),
                                      "env@tenant" -> Seq()))
    val expectedResponse =   EsResponse(Seq(AliasInfo("dev@oms", "Alias_dev@oms", Some("1"), Some("1")),
                                            AliasInfo("dev@super", "Alias_dev@super", Some("2"), Some("2"))))
    doReturn(getAliasesResponse).when(esClient).getAliases(any())

    esClient.catAliases shouldEqual expectedResponse
  }

  behavior of "#catAliasIndexMap"
  it should "Get a map of index to a list of aliases in the cluster." in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val getAliasesResponse = EsResponse(Map("dev@oms" -> Seq(AliasInfo("dev@oms", "Alias_dev@oms", Some("1"), Some("1"))),
      "dev@super" -> Seq(AliasInfo("dev@super", "Alias_dev@super", Some("2"), Some("2"))),
      "env@tenant" -> Seq()))

    doReturn(getAliasesResponse).when(esClient).getAliases(any())
    esClient.catAliasIndexMap shouldEqual getAliasesResponse
  }

  behavior of "#getAliasesByIndex"
  it should "Get a list of aliases for an index." in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val aliasList = Seq(AliasInfo("dev@oms", "Alias_dev@oms1", Some("1"), Some("1")),
                        AliasInfo("dev@oms", "Alias_dev@oms2", Some("2"), Some("2")))
    val getAliasesResponse = EsResponse(Map("dev@oms" -> Seq(AliasInfo("dev@oms", "Alias_dev@oms1", Some("1"), Some("1")),
                                                             AliasInfo("dev@oms", "Alias_dev@oms2", Some("2"), Some("2")))))

    doReturn(getAliasesResponse).when(esClient).getAliases(any())

    esClient.getAliasesByIndex("dev@oms") shouldEqual EsResponse(aliasList)
  }

  behavior of "#getAliases"
  it should "Gets a EsResult of Map of index to aliases for given GetAliases object." in {

    val esClient = spy(new EsClient(mock[JestClient]))
    val toResultResponse = EsResponse(Map("dev@super" -> ReturnedAliases(Map("alias_1_dev@super" -> RoutingInfo(Some("1"),Some("1")),
                                                                            "alias_2_dev@super" -> RoutingInfo(Some("2"),Some("2")))),
                                          "dev@oms" -> ReturnedAliases(Map("alias_1_dev@oms" -> RoutingInfo(Some("1"),Some("1")),
                                                                            "alias_2_dev@oms" -> RoutingInfo(Some("2"),Some("2")))),
                                          "env@tenant" -> ReturnedAliases(Map())))

    val expectedResponse = EsResponse(Map("dev@oms" -> Seq(AliasInfo("dev@oms", "alias_1_dev@oms", Some("1"), Some("1")),
                                                            AliasInfo("dev@oms", "alias_2_dev@oms", Some("2"), Some("2"))),
                                          "dev@super" -> Seq(AliasInfo("dev@super", "alias_1_dev@super", Some("1"), Some("1")),
                                                              AliasInfo("dev@super", "alias_2_dev@super", Some("2"), Some("2"))),
                                          "env@tenant" -> Seq()))

    doReturn(toResultResponse).when(esClient)
      .toEsResult[Map[String, ReturnedAliases]](any(), anyBoolean())(any[Manifest[Map[String, ReturnedAliases]]])

    esClient.getAliases(esClient.buildGetAliases(None)) shouldEqual expectedResponse
  }

  behavior of "#getAliasesByName"
  it should "Get a specific alias back from ES." in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val toResultResponse = EsResponse(Map("dev@super" -> ReturnedAliases(Map("alias_1_dev@super" -> RoutingInfo(Some("1"),Some("1"))))))
    val expectedResponse =   EsResponse(Seq(AliasInfo("dev@super", "alias_1_dev@super", Some("1"), Some("1"))))

    doReturn(toResultResponse).when(esClient)
      .toEsResult[Map[String, ReturnedAliases]](any(), anyBoolean())(any[Manifest[Map[String, ReturnedAliases]]])

    esClient.getAliasesByName(Seq("alias_1_dev@super")) shouldEqual expectedResponse
  }

  behavior of "#buildGetAliases"
  it should "Gets the GetAliases object for all indices in the cluster." in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val expectedGetAliases = new GetAliases.Builder().build()

    esClient.buildGetAliases(None).getURI shouldEqual expectedGetAliases.getURI
  }

  it should "Gets the GetAliases object for a specific index." in {
    val esClient = spy(new EsClient(mock[JestClient]))
    val indexName = "dev@oms"
    val expectedGetAliases = new GetAliases.Builder().addIndex(indexName).build()

    esClient.buildGetAliases(Some(indexName)).getURI shouldEqual expectedGetAliases.getURI
  }

  behavior of "#AliasBuilder"
  it should "do stuff" in {
    val esClient = new EsClient(mock[JestClient])
    val builder = new AliasBuilder(toAdd, toRemove)

    val expectedActions = Map("actions" ->
      Seq(
        Map("add" -> Map("index" -> "index1", "alias" -> "dev@test11", "index_routing" -> "1", "search_routing" -> "1")),
        Map("add" -> Map("index" -> "index2", "alias" -> "dev@test12")),
        Map("remove" -> Map("index" -> "index1", "alias" -> "dev@test13", "index_routing" -> "1", "search_routing" -> "1"))
      )
    )

    builder.actions shouldEqual expectedActions
  }

  behavior of "#AliasAction"
  it should "Have a REST Method type of POST" in {
    val aliasAction = new AliasBuilder(Nil, Nil).build
    aliasAction.getRestMethodName shouldEqual "POST"
  }

  it should "Properly construct JSON" in {
    val aliasAction = new AliasBuilder(toAdd, toRemove).build

    val expectedJson = """{
        |    "actions" : [
        |        {"add" : {"index" : "index1", "alias" : "dev@test11", "index_routing" : "1", "search_routing" : "1"}},
        |        {"add" : {"index" : "index2", "alias" : "dev@test12"}},
        |        {"remove" : {"index" : "index1", "alias" : "dev@test13", "index_routing" : "1", "search_routing" : "1"}}
        |    ]
        |}""".stripMargin

    JsonUtils.equals(aliasAction.getData(new Gson()), expectedJson) shouldEqual true
  }

  it should "Have the proper URI" in {
    val aliasAction = new AliasBuilder(Nil, Nil).build
    aliasAction.getURI shouldEqual "_aliases"
  }

  behavior of "#AliasInfo"
  it should "Create a Map from an AliasInfo" in {
    val aliasInfo = AliasInfo("index1", "alias1", Some("123"), Some("45"))
    val expectedMap = Map("index" -> "index1", "alias" -> "alias1", "index_routing"  -> "123", "search_routing"  -> "45")
    aliasInfo.toMap shouldEqual expectedMap
  }

  it should "Create a Map from an AliasInfo without routing" in {
    val aliasInfo = AliasInfo("index1", "alias1")
    val expectedMap = Map("index" -> "index1", "alias" -> "alias1")
    aliasInfo.toMap shouldEqual expectedMap
  }
}
