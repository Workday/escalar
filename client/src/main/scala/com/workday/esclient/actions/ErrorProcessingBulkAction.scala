package com.workday.esclient.actions

import com.google.gson.Gson
import io.searchbox.core.{Bulk, BulkResult}
import org.slf4j.LoggerFactory

class ErrorProcessingBulkBuilder extends Bulk.Builder {
  override def build: ErrorProcessingBulkAction = new ErrorProcessingBulkAction(this)
}

class ErrorProcessingBulkAction(builder: ErrorProcessingBulkBuilder) extends Bulk(builder) {
  private[ErrorProcessingBulkAction] val logger = LoggerFactory.getLogger(classOf[ErrorProcessingBulkAction])

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
