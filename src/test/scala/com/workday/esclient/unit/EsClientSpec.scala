package com.workday.esclient.unit

import com.google.gson.{JsonObject, JsonParser}
import com.workday.esclient.actions.CatAction
import com.workday.esclient.{AliasAction, AliasInfo, EsClient, GenericAliasInfo}
import io.searchbox.action.{AbstractAction, BulkableAction}
import io.searchbox.client.{JestClient, JestResult}
import io.searchbox.core._
import io.searchbox.indices.CreateIndex
import io.searchbox.indices.aliases.GetAliases
import org.mockito.Matchers._
import org.mockito.Mockito._

class EsClientSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll
  with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar  {

  behavior of "#createEsClient"
  it should "return a client" in {
    val esUrl = "http://localhost:18103"
    val client = EsClient.createEsClient(esUrl)
    client should not be null
  }

  behavior of "#shutdownClient"
  it should "shutdown" in {
    val mockJestClient = mock[JestClient]
    val client = new EsClient(mockJestClient)
    client.shutdownClient()
    verify(mockJestClient, times(1)).shutdownClient()
  }
}

/**
  * Utility functions for other EsClient tests
  */
object EsClientSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll
  with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar  {

  def toJsonObject(jsonStr: String) = {
    new JsonParser().parse(jsonStr).getAsJsonObject
  }

  class EsClientWithMockedEs(jest: JestClient = mock[JestClient]) extends EsClient(jest) {

    val GetMock = mock[Get]
    val MultiGetMock = mock[MultiGet]
    val UpdateMock = mock[Update]
    val DeleteByQueryMock = mock[DeleteByQuery]
    val CreateIndexMock = mock[CreateIndex]
    val ModifyAliasesMock = mock[AliasAction]
    val BulkMock = mock[Bulk]
    val SearchMock = mock[Search]
    val ScrolledSearchMock = mock[Search]
    val CatActionMock = mock[CatAction]
    val CatIndicesMock = mock[Cat]
    val GetAliasesMock = mock[GetAliases]
    val CatShardsMock = mock[CatAction]

    override def buildUpdateAction(index: String, typeName: String, id: String, payload: String) = UpdateMock
    override def buildDeleteByQueryAction(index: String, query: Option[String]) = DeleteByQueryMock
    override def buildGetAction(index: String, id: String) = GetMock
    override def buildMultiGetAction(index: String, typeName: String, ids: Seq[String], params: Map[String, Any] = Map()) = MultiGetMock
    override def buildBulkAction(actions: Seq[BulkableAction[DocumentResult]]) = BulkMock
    override def buildSearchAction(query: String, index: String, typeName: String, params: Map[String, Any]) = SearchMock
    override def buildCreateIndex(index: String, settings: Option[Map[String, Any]] = None) = CreateIndexMock
    override def buildModifyAliases(toAdd: Seq[GenericAliasInfo], toRemove: Seq[GenericAliasInfo]) = ModifyAliasesMock
    override def buildCatAction(catAction: String, indexName: String = "") = CatActionMock
    override def buildCatIndices() = CatIndicesMock
    override def buildGetAliases(index: Option[String]) = GetAliasesMock
    override def buildCatShards(indexName: String = "") = CatShardsMock

    def whenGet(jsonString: String, expectJsonError: Boolean = false): EsClientWithMockedEs = mockJestResult(jsonString, GetMock, expectJsonError)
    def whenMultiGet(jsonString: String): EsClientWithMockedEs = mockJestResult(jsonString, MultiGetMock)
    def whenCreateIndex(jsonString: String): EsClientWithMockedEs = mockJestResult(jsonString, CreateIndexMock)
    def whenModifyAliases(jsonString: String): EsClientWithMockedEs = mockJestResult(jsonString, ModifyAliasesMock)
    def whenUpdate(jsonString: String): EsClientWithMockedEs = mockJestResult(jsonString, UpdateMock)
    def whenDeleteByQuery(jsonString: String): EsClientWithMockedEs = mockJestResult(jsonString, DeleteByQueryMock)
    def whenBulk(jsonString: String): EsClientWithMockedEs = mockJestResult(jsonString, BulkMock)
    def whenSearch(jsonString: String): EsClientWithMockedEs = mockSearchResult(jsonString)
    def whenScrolledSearch(jsonString: String): EsClientWithMockedEs = mockScrolledSearchResult(jsonString)
    def whenCatAction(jsonString: String): EsClientWithMockedEs = mockCatActionResult(jsonString)
    def whenCatIndices(jsonString: String): EsClientWithMockedEs = mockCatIndicesResult(jsonString)
    def whenGetAliases(jsonString: String): EsClientWithMockedEs = mockGetAliasesResult(jsonString)
    def whenCatShards(jsonString: String): EsClientWithMockedEs = mockCatShardsResult(jsonString)

    def whenAny(jsonString: String) = {
      mockResult[JestResult](jsonString, toJsonObject(jsonString), matchAny = true)
    }

    def mockJestResult[T <: JestResult : Manifest](jsonString: String, mockAction: AbstractAction[T], expectJsonError: Boolean = false) = {
      val jsonObject = if (!expectJsonError) toJsonObject(jsonString) else toJsonObject("{}")
      mockResult[T](jsonString, jsonObject, mockAction)
    }

    def mockSearchResult(jsonString: String) = {
      mockResult[SearchResult](jsonString, toJsonObject(jsonString), SearchMock)
    }

    def mockScrolledSearchResult(jsonString: String) = {
      mockResult[SearchResult](jsonString, toJsonObject(jsonString), ScrolledSearchMock)
    }

    def mockCatActionResult(jsonString: String) = {
      mockResult[CatResult](jsonString, toCatResultJsonObject(jsonString), CatActionMock)
    }

    def mockCatIndicesResult(jsonString: String) = {
      mockResult[CatResult](jsonString, toCatResultJsonObject(jsonString), CatIndicesMock)
    }

    def mockGetAliasesResult(jsonString: String) = {
      mockResult[JestResult](jsonString, toJsonObject(jsonString), GetAliasesMock)
    }

    def mockCatShardsResult(jsonString: String) = {
      mockResult[CatResult](jsonString, toCatResultJsonObject(jsonString), CatShardsMock)
    }

    private[this] def mockResult[T <: JestResult : Manifest](jsonString: String,
      jsonObject: JsonObject,
      mockAction: AbstractAction[T] = mock[AbstractAction[JestResult]],
      matchAny: Boolean = false
    ) = {
      val mockResult = mock[T]
      when(jest.execute(mockAction)).thenReturn(mockResult)
      if (matchAny) {
        when(jest.execute(any())).thenReturn(mockResult)
      }
      when(mockResult.getJsonString).thenReturn(jsonString)
      when(mockResult.getJsonObject).thenReturn(jsonObject)
      this
    }

    private[this] def toCatResultJsonObject(jsonStr: String): JsonObject = {
      val result: JsonObject = new JsonObject
      if (Option(jsonStr).nonEmpty && !jsonStr.trim.isEmpty) {
        result.add("result", new JsonParser().parse(jsonStr).getAsJsonArray)
      }
      result
    }
  }
}
