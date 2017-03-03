/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import com.google.gson.Gson
import io.searchbox.core.{Bulk, BulkResult}
import org.slf4j.LoggerFactory

/**
  * Builder class for error processing bulk action.
  */
class ErrorProcessingBulkBuilder extends Bulk.Builder {
  /**
    * Builds [[com.workday.esclient.actions.ErrorProcessingBulkAction]]
    * @return
    */
  override def build: ErrorProcessingBulkAction = new ErrorProcessingBulkAction(this)
}

/**
  * Action class for error processing bulk action.
  * @param builder [[com.workday.esclient.actions.ErrorProcessingBulkBuilder]]
  */
class ErrorProcessingBulkAction(builder: ErrorProcessingBulkBuilder) extends Bulk(builder) {
  private[ErrorProcessingBulkAction] val logger = LoggerFactory.getLogger(classOf[ErrorProcessingBulkAction])

  /**
    * Creates a new Elasticsearch result from result.
    * @param result [[io.searchbox.core.BulkResult]] ES result.
    * @param responseBody String ES response body.
    * @param statusCode Int status code from ES.
    * @param reasonPhrase String reason given for status code.
    * @param gson [[com.google.gson.Gson]] JSON object.
    * @return [[io.searchbox.core.BulkResult]] from ES.
    */
  override def createNewElasticSearchResult(result: BulkResult, responseBody: String, statusCode: Int, reasonPhrase: String, gson: Gson): BulkResult = {
    try {
      super.createNewElasticSearchResult(result, responseBody, statusCode, reasonPhrase, gson)
    } catch {
      case e: Exception =>
        val oneLineBody = responseBody.replace("\n", "\n! ")
        logger.warn(s"Error parsing Bulk response.  statusCode=$statusCode, responseBody='$oneLineBody'")
        throw e
    }
  }
}
