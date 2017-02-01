package com.workday.esclient

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.common.annotations.VisibleForTesting
import com.google.gson.Gson
import com.workday.esclient.actions.GetAliasByNameBuilder
import io.circe.syntax._
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}
import io.searchbox.client.JestResult
import io.searchbox.indices.aliases.GetAliases

/**
  * Elasticsearch Alias APIs
  */
trait EsAliases extends JestUtils {
  /**
    * Client APIs
    */
  def createAliases(aliases: Seq[AliasInfo]): EsResult[Acknowledgement] = modifyAliases(aliases, Nil)

  def deleteAliases(aliases: Seq[AliasInfo]): EsResult[Acknowledgement] = modifyAliases(Nil, aliases)

  @VisibleForTesting
  private[esclient] def buildModifyAliases(toAdd: Seq[AliasInfo], toRemove: Seq[AliasInfo]): AliasAction = {
    new AliasBuilder(toAdd, toRemove).build
  }

  def modifyAliases(toAdd: Seq[AliasInfo], toRemove: Seq[AliasInfo]): EsResult[Acknowledgement] = {
    val jestResult = jest.execute(buildModifyAliases(toAdd, toRemove))
    toEsResult[Acknowledgement](jestResult)
  }

  def catAliases: EsResult[Seq[AliasInfo]] = {
    catAliasIndexMap.map[Seq[AliasInfo]]((indexAliasMap: Map[String, Seq[AliasInfo]]) => indexAliasMap.values.flatten.toSeq)
  }

  def catAliasIndexMap: EsResult[Map[String, Seq[AliasInfo]]] = {
    val catAliasesForCluster = buildGetAliases(None)
    getAliases(catAliasesForCluster)
  }

  def getAliasesByIndex(index: String): EsResult[Seq[AliasInfo]] = {
    val getAliasesByIndex = buildGetAliases(Some(index))
    getAliases(getAliasesByIndex).map[Seq[AliasInfo]]((indexAliasMap: Map[String, Seq[AliasInfo]]) => indexAliasMap.values.flatten.toSeq)
  }

  def getAliasesByName(aliasNames: Seq[String]): EsResult[Seq[AliasInfo]] = {
    val jestResult: JestResult = jest.execute(new GetAliasByNameBuilder(aliasNames).build())
    parseJestResult(jestResult).map[Seq[AliasInfo]]((indexAliasMap: Map[String, Seq[AliasInfo]]) => indexAliasMap.values.flatten.toSeq)
  }

  private[esclient] def parseJestResult(result: JestResult): EsResult[Map[String, Seq[AliasInfo]]] = {
    toEsResult[Map[String, ReturnedAliases]](result).map[Map[String, Seq[AliasInfo]]](
      (esReturnedMap) => {
        esReturnedMap.map { case (index, returnedAlias) =>
          index ->
            returnedAlias.aliases.map {
              case (aliasName, routingInfo) =>
                AliasInfo(index, aliasName, routingInfo.indexRouting, routingInfo.searchRouting)
            }.toSeq
        }
      }
    )
  }

  @VisibleForTesting
  private[esclient] def getAliases(getAliases: GetAliases): EsResult[Map[String, Seq[AliasInfo]]] = {
    val jestResult: JestResult = jest.execute(getAliases)
    parseJestResult(jestResult)
  }

  @VisibleForTesting
  private[esclient] def buildGetAliases(index: Option[String]): GetAliases = {
    index match {
      case Some(indexName: String) => new GetAliases.Builder().addIndex(indexName).build()
      case None => new GetAliases.Builder().build()
    }
  }
}

/**
  * Action Classes
  */
class AliasBuilder(toAdd: Seq[AliasInfo], toRemove: Seq[AliasInfo]) extends AbstractMultiTypeActionBuilder[AliasAction, AliasBuilder] {
  val actions = Map(
    "actions" -> (
      toAdd.map { aliasInfo => Map("add" -> aliasInfo.toMap) } ++
      toRemove.map { aliasInfo => Map("remove" -> aliasInfo.toMap) }
    )
  )

  override def build: AliasAction = new AliasAction(this)
}

class AliasAction(builder: AliasBuilder) extends GenericResultAbstractAction(builder) {
  setURI(buildURI)
  override def getRestMethodName: String = "POST"
  override def getData(gson: Gson): String = JsonUtils.toJson(builder.actions)
  protected override def buildURI: String = "_aliases"
}

/**
  * Case Classes
  */
case class ReturnedAliases(aliases: Map[String, RoutingInfo])
case class RoutingInfo(indexRouting: Option[String], searchRouting: Option[String])

@JsonIgnoreProperties(ignoreUnknown = true)
case class AliasInfo(
  index: String,
  alias: String,
  indexRouting: Option[String] = None,
  searchRouting: Option[String] = None
){
  lazy val toMap: Map[String, Any] = Map("index" -> index, "alias" -> alias) ++
    indexRouting.map { routingVal => Map("index_routing" -> routingVal) }.getOrElse(Map()) ++
    searchRouting.map { routingVal => Map("search_routing" -> routingVal) }.getOrElse(Map())
}
