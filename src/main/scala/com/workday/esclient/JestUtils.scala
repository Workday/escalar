/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import com.google.common.annotations.VisibleForTesting
import io.searchbox.client.{JestClient, JestResult}

/**
  * Enables access to a single JestClient across many EsClient APIs (modularized into traits).
  * The jest def here is overridden in EsClient.scala with the val jest.
  *
  * It also includes other helper functions for parsing responses from the Jest client.
  */
trait JestUtils {
  /**
    * @return JestClient
    */
  def jest: JestClient // overridden in EsClient.scala with a val

  /**
    * Returns [[com.workday.esclient.EsResult]][T] from JSON Jest response.
    * @param jestResult Jest response to be handled
    * @param allowError Boolean value whether to parse error responses
    * @tparam T implicit manifest
    * @return EsResult[T]
    */
  @VisibleForTesting
  def toEsResult[T: Manifest](jestResult: JestResult, allowError: Boolean = false): EsResult[T] = {
    handleJestResult(jestResult, allowError) { successfulJestResult =>
      JsonUtils.fromJson[T](successfulJestResult.getJsonString)
    }
  }

  /**
    * Returns successful EsResult[T] or error response on JSON parsing/mapping failure.
    * @param allowError - multiGet returns a GetResponse with an error field instead of a source, so we should report that normally
    * @tparam J we need J here to allow things like jest's CountResult
    * @return EsResult[T] either EsResponse[T] or an error message type
    */
  protected def handleJestResult[J <: JestResult, T](jestResult: J, allowError: Boolean = false)(responseHandler: J => T): EsResult[T] = {
    // Return a EsInvalidResponse if both the JSON Object and String are null.
    if(Option(jestResult.getJsonObject).isEmpty && Option(jestResult.getJsonString).isEmpty) {
      EsInvalidResponse("Unable to Parse JSON into the given class because result contains all NULL entries.")
    }
    else if(!allowError && (Option(jestResult.getJsonObject).nonEmpty && !jestResult.getJsonObject.entrySet().isEmpty)
      && jestResult.getJsonObject.has("error")) {
      JsonUtils.fromJson[EsError_1_7](jestResult.getJsonString)
    }
    else {
      try {
        // Set the JSON string if JSON object is set but the JSON string is null.
        if(Option(jestResult.getJsonString).isEmpty) {
          jestResult.setJsonString(jestResult.getJsonObject.toString)
        }
        EsResponse(responseHandler(jestResult))
      }
      catch {
        // Mapping exception occurs if JSON string = ""
        case mappingException: JsonMappingException => {
          EsInvalidResponse("Unable to Map JSON to the given Class.")
        }
        // Parse exception occurs when JSON string cannot be mapped to the fields of the class.
        case parsingException: JsonParseException => {
          EsInvalidResponse("Unable to Parse JSON into the given Class.")
        }
      }
    }
  }
}

/**
  * Generic trait for ES acknowledgement responses.
  */
trait Acknowledgement {
  def acknowledged: Boolean
}

/**
  * Case class for acknowledgment EsResults.
  * @version 1.7
  * @param acknowledged boolean value for acknowledgment
  */
case class Acknowledgement_1_7(
  acknowledged: Boolean
) extends Acknowledgement

// We'll likely want to consider adding the original jestResult as a parameter
/**
  * Trait for wrapping Elasticsearch response types.
  * @tparam T response type
  */
trait EsResult[+T] {
  def get: T

  def map[R](f: T => R): EsResult[R] = this match {
    case EsResponse(value) => EsResponse(f(value))
    case e: EsInvalidResponse => e
    case e: EsError => e
  }
}

/**
  * Generic trait for all ES error responses.
  */
trait EsError extends EsResult[Nothing] {
  def get: Nothing
}

/**
  * Case class for an Elasticsearch error response.
  * @version 1.7
  * @param error  error message
  * @param status status code
  * @example ES JSON Response
  *          {
  *          "error": "DocumentMissingException[\[test][0] [person][5]: document missing]",
  *          "status": 404
  *          }
  */
case class EsError_1_7(error: String, status: Int) extends EsError {
  def get: Nothing = throw new NoSuchElementException(error + ", status " + status)
}

/**
  * Case class for an Elasticsearch invalid response.
  * @param msg ES response message
  */
case class EsInvalidResponse(msg: String) extends EsError {
  def get: Nothing = throw new NoSuchElementException(msg)
}

/**
  * Case class for an Elasticsearch valid response.
  * @param value value instance to wrap in case class
  * @tparam T response type
  */
case class EsResponse[T](value: T) extends EsResult[T] {
  def get: T = value
}
