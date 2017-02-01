package com.workday.esclient

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.common.annotations.VisibleForTesting
import io.searchbox.indices.mapping.GetMapping
import io.searchbox.indices.{CloseIndex, CreateIndex, DeleteIndex}

import scala.collection.JavaConverters.asJavaCollectionConverter

/**
  * Elasticsearch Index Metadata APIs
  */
trait EsIndexingMeta extends JestUtils {

  def createIndex(index: String, settings: Option[Map[String, Any]] = None): EsResult[Acknowledgement] = {
    val jestResult = jest.execute(buildCreateIndex(index, settings))
    toEsResult[Acknowledgement](jestResult)
  }

  def deleteIndex(index: String): EsResult[Acknowledgement] = {
    val jestResult = jest.execute(new DeleteIndex.Builder(index).build())
    toEsResult[Acknowledgement](jestResult)
  }

  def closeIndex(index: String): EsResult[Acknowledgement] = {
    val closeAction = new CloseIndex.Builder(index).build()
    val jestResult = jest.execute(closeAction)
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Gets all of the mappings for an index
    * TODO: create variants for specific fields
    */
  def getMappingsForIndex(index: String): EsResult[IndexMappings] = {
    getMappingsForIndices(Seq(index), Nil).map(_.apply(index))
  }

  def getMappingsForIndex(index: String, typeName: String): EsResult[IndexTypeMappings] = {
    getMappingsForIndices(Seq(index), Seq(typeName)).map(_.apply(index).mappings(typeName))
  }

  def getMappingsForIndices(indices: Seq[String], typeNames: Seq[String]): EsResult[Map[String, IndexMappings]] = {
    val jestResult = jest.execute(new GetMapping.Builder().addIndex(indices.asJavaCollection).addType(typeNames.asJavaCollection).build())
    handleJestResult(jestResult) { successfulJestResult =>
      JsonUtils.fromJson[Map[String, IndexMappings]](successfulJestResult.getJsonString)
    }
  }

  @VisibleForTesting
  private[esclient] def buildCreateIndex(index: String, settings: Option[Map[String, Any]] = None): CreateIndex = {
    val builder = new CreateIndex.Builder(index)
    // Settings have to be converted into a json string before being passed to the builder
    // See an example here: https://github.com/searchbox-io/Jest/blob/master/jest/src/test/java/io/searchbox/indices/CreateIndexIntegrationTest.java
    builder.settings(JsonUtils.toJson(settings.getOrElse(Map())))
    builder.build
  }
}

/**
  * @param mappings map from ES document type -> IndexTypeMappings
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class IndexMappings(mappings: Map[String, IndexTypeMappings])

/**
  * @param properties map from fieldname -> FieldProperties
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class IndexTypeMappings(properties: Map[String, IndexFieldProperties])

@JsonIgnoreProperties(ignoreUnknown = true)
case class IndexFieldProperties(`type`: String)
