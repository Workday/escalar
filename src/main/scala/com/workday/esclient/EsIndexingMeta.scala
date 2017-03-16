/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.common.annotations.VisibleForTesting
import io.searchbox.indices.mapping.GetMapping
import io.searchbox.indices.{CloseIndex, CreateIndex, DeleteIndex}

import scala.collection.JavaConverters.asJavaCollectionConverter

/**
  * Trait wrapping Elasticsearch Index Metadata APIs
  */
trait EsIndexingMeta extends JestUtils {

  /**
    * Creates an Elasticsearch index and returns an EsResult acknowledgment.
    * @param index String index to create in ES.
    * @param settings Optional map of settings for the index.
    * @return EsResult wrapping an ES acknowledgment.
    */
  def createIndex(index: String, settings: Option[Map[String, Any]] = None): EsResult[GenericAcknowledgement] = {
    val jestResult = jest.execute(buildCreateIndex(index, settings))
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Deletes an Elasticsearch index and returns an EsResult acknowledgment.
    * @param index String index to delete from ES.
    * @return EsResult wrapping an ES acknowledgment.
    */
  def deleteIndex(index: String): EsResult[GenericAcknowledgement] = {
    val jestResult = jest.execute(new DeleteIndex.Builder(index).build())
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Closes an Elasticsearch index and returns an EsResult acknowledgment.
    * Closed indices have very little overhead on the cluster and are blocked from read/write operations.
    * @param index String index to close in ES.
    * @return EsResult wrapping an ES acknowledgment.
    */
  def closeIndex(index: String): EsResult[GenericAcknowledgement] = {
    val closeAction = new CloseIndex.Builder(index).build()
    val jestResult = jest.execute(closeAction)
    toEsResult[Acknowledgement](jestResult)
  }

  /**
    * Gets all of the mappings for an Elasticsearch index.
    * TODO: create variants for specific fields.
    * @param index String index to get mappings from.
    * @return EsResult wrapping the index mappings.
    */
  def getMappingsForIndex(index: String): EsResult[IndexMappings] = {
    getMappingsForIndices(Seq(index), Nil).map(_.apply(index))
  }

  /**
    * Gets all of the mappings for an Elasticsearch index.
    * @param index String index to get mappings from.
    * @param typeName String ES type name.
    * @return EsResult wrapping the index mappings.
    */
  def getMappingsForIndex(index: String, typeName: String): EsResult[IndexTypeMappings] = {
    getMappingsForIndices(Seq(index), Seq(typeName)).map(_.apply(index).mappings(typeName))
  }

  /**
    * Gets all of the mapping for a sequence of indices in Elasticsearch.
    * @param indices Sequence of ES indices.
    * @param typeNames Sequence of ES type names.
    * @return EsResult wrapping a map of indices and their mappings.
    */
  def getMappingsForIndices(indices: Seq[String], typeNames: Seq[String]): EsResult[Map[String, IndexMappings]] = {
    val jestResult = jest.execute(new GetMapping.Builder().addIndex(indices.asJavaCollection).addType(typeNames.asJavaCollection).build())
    handleJestResult(jestResult) { successfulJestResult =>
      JsonUtils.fromJson[Map[String, IndexMappings]](successfulJestResult.getJsonString)
    }
  }

  /**
    * Returns a CreateIndex object
    * @param index String ES index.
    * @param settings String optional settings for index.
    * @return CreateIndex object.
    */
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
  * Case class wrapping Elasticsearch indices mappings.
  * @param mappings map from ES document type -> IndexTypeMappings
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class IndexMappings(mappings: Map[String, IndexTypeMappings])

/**
  * Case class wrapping Elasticsearch indices mappings.
  * @param properties map from fieldname -> FieldProperties
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class IndexTypeMappings(properties: Map[String, IndexFieldProperties])

/**
  * Case class wrapping Elasticsearch index field properties.
  * @param `type` String ES type name.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class IndexFieldProperties(`type`: String)
