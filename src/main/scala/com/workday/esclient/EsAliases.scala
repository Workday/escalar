/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.common.annotations.VisibleForTesting
import com.google.gson.Gson
import com.workday.esclient.actions.GetAliasByNameBuilder
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}
import io.searchbox.client.JestResult
import io.searchbox.indices.aliases.GetAliases

/**
  * Trait wrapping Elasticsearch Alias APIs
  */
trait EsAliases extends JestUtils {
  /**
    * Creates aliases for given indices in Elasticsearch.
    * @param aliases Sequence of aliases including alias names and indices to map to.
    * @return EsResult wrapping an ES acknowledgment.
    */
  def createAliases(aliases: Seq[AliasInfo]): EsResult[Acknowledgement] = modifyAliases(aliases, Nil)

  /**
    * Deletes aliases from Elasticsearch.
    * @param aliases Sequence of aliases including alias names and indices to map to.
    * @return EsResult wrapping an ES acknowledgment.
    */
  def deleteAliases(aliases: Seq[AliasInfo]): EsResult[Acknowledgement] = modifyAliases(Nil, aliases)

  /**
    * Returns an AliasAction object to alter aliases in Elasticsearch.
    * @param toAdd Sequence of aliases to add to ES.
    * @param toRemove Sequence of aliases to remove from ES.
    * @return AliasAction object.
    */
  @VisibleForTesting
  private[esclient] def buildModifyAliases(toAdd: Seq[AliasInfo], toRemove: Seq[AliasInfo]): AliasAction = {
    new AliasBuilder(toAdd, toRemove).build
  }

  /**
    * Modifies aliases in Elasticsearch and returns an acknowledgment.
    * @param toAdd Sequences of aliases to add to ES.
    * @param toRemove Sequences of aliases to remove from ES>
    * @return EsResult wrapping an ES acknowledgment.
    */
  def modifyAliases(toAdd: Seq[AliasInfo], toRemove: Seq[AliasInfo]): EsResult[Acknowledgement] = {
    val jestResult = jest.execute(buildModifyAliases(toAdd, toRemove))
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Cats all Elasticsearch alias info and returns an EsResult wrapping alias information.
    * @return EsResult wrapping a sequence of AliasInfo objects.
    */
  def catAliases: EsResult[Seq[AliasInfo]] = {
    catAliasIndexMap.map[Seq[AliasInfo]]((indexAliasMap: Map[String, Seq[AliasInfo]]) => indexAliasMap.values.flatten.toSeq)
  }

  /**
    * Returns a map of Elasticsearch alias info.
    * @return EsResult wrapping a map of AliasInfo.
    */
  def catAliasIndexMap: EsResult[Map[String, Seq[AliasInfo]]] = {
    val catAliasesForCluster = buildGetAliases(None)
    getAliases(catAliasesForCluster)
  }

  /**
    * Gets Elasticsearch aliases by index and return a sequence of AliasInfo.
    * @param index String index to get aliases for.
    * @return EsResult wrapping the aliases.
    */
  def getAliasesByIndex(index: String): EsResult[Seq[AliasInfo]] = {
    val getAliasesByIndex = buildGetAliases(Some(index))
    getAliases(getAliasesByIndex).map[Seq[AliasInfo]]((indexAliasMap: Map[String, Seq[AliasInfo]]) => indexAliasMap.values.flatten.toSeq)
  }

  /**
    * Gets Elasticsearch alias info from a sequence of alias names.
    * @param aliasNames Sequence of ES alias names.
    * @return EsResult wrapping the aliases.
    */
  def getAliasesByName(aliasNames: Seq[String]): EsResult[Seq[AliasInfo]] = {
    val jestResult: JestResult = jest.execute(new GetAliasByNameBuilder(aliasNames).build())
    parseJestResult(jestResult).map[Seq[AliasInfo]]((indexAliasMap: Map[String, Seq[AliasInfo]]) => indexAliasMap.values.flatten.toSeq)
  }

  /**
    * Parses a Jest result and returns an EsResult wrapping a map of alias info.
    * @param result JestResult result to parse.
    * @return EsResult wrapping map of alias info.
    */
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

  /**
    * Gets aliases from Elasticsearch and returns an EsResult with a map of alias info.
    * @param getAliases GetAliases object to pass into the Jest client.
    * @return EsResult wrapping a map of alias info.
    */
  @VisibleForTesting
  private[esclient] def getAliases(getAliases: GetAliases): EsResult[Map[String, Seq[AliasInfo]]] = {
    val jestResult: JestResult = jest.execute(getAliases)
    parseJestResult(jestResult)
  }

  /**
    * Returns a GetAliases object for making request to Elasticsearch.
    * @param index Optional string for ES index name.
    * @return GetAliases object.
    */
  @VisibleForTesting
  private[esclient] def buildGetAliases(index: Option[String]): GetAliases = {
    index match {
      case Some(indexName: String) => new GetAliases.Builder().addIndex(indexName).build()
      case None => new GetAliases.Builder().build()
    }
  }
}

/**
  * Class for wrapping alias add and remove actions in Elasticsearch.
  * @param toAdd Sequence of aliases to add.
  * @param toRemove Sequence of aliases to remove.
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

/**
  * Class for wrapping alias requests in Elasticsearch.
  * @param builder AliasBuilder with alias info for actions.
  */
class AliasAction(builder: AliasBuilder) extends GenericResultAbstractAction(builder) {
  setURI(buildURI)
  override def getRestMethodName: String = "POST"
  override def getData(gson: Gson): String = JsonUtils.toJson(builder.actions)
  protected override def buildURI: String = "_aliases"
}

/**
  * Case class wrapping aliases returned from Elasticsearch.
  * @param aliases Map of aliases and RoutingInfo
  */
case class ReturnedAliases(aliases: Map[String, RoutingInfo])

/**
  * Case class wrapping Elasticsearch routing information between alias and indices.
  * @param indexRouting Optional string for ES index routing.
  * @param searchRouting Optional string for ES search routing.
  */
case class RoutingInfo(indexRouting: Option[String], searchRouting: Option[String])

/**
  * Generic trait wrapping Elasticsearch alias information.
  */
trait GenericAliasInfo{
  /**
    * @return String index for the alias.
    */
  def index: String

  /**
    * @return String alias name.
    */
  def alias: String

  /**
    * @return Optional string for the ES index routing.
    */
  def indexRouting: Option[String]

  /**
    * @return Optional string for the ES search routing.
    */
  def searchRouting: Option[String]
}

/**
  * Case class wrapping Elasticsearch alias information.
  * @param index String index for the alias.
  * @param alias String alias name.
  * @param indexRouting Optional string for the ES index routing.
  * @param searchRouting Optional string for the ES search routing.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class AliasInfo(
  index: String,
  alias: String,
  indexRouting: Option[String] = None,
  searchRouting: Option[String] = None
) extends GenericAliasInfo{
  lazy val toMap: Map[String, Any] = Map("index" -> index, "alias" -> alias) ++
    indexRouting.map { routingVal => Map("index_routing" -> routingVal) }.getOrElse(Map()) ++
    searchRouting.map { routingVal => Map("search_routing" -> routingVal) }.getOrElse(Map())
}


