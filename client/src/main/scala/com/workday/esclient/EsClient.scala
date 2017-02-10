package com.workday.esclient

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}

/**
  * Factory for [[com.workday.esclient.EsClient]] instances
  */
object EsClient {
  /*
   * You thought Scala vals were already final?  Think again.
   * http://stackoverflow.com/questions/13412386/why-are-private-val-and-private-final-val-different
   * For whatever reason, this has the side-effect of allowing these to be used in annotations.
   */
  //$COVERAGE-OFF$
  private[esclient] final val _SCORE = "_score"
  private[esclient] final val _SOURCE = "_source"
  private[esclient] final val _EXPLANATION = "_explanation"
  private[esclient] final val _INDEX = "_index"
  private[esclient] final val _TYPE = "_type"
  private[esclient] final val _ID = "_id"
  private[esclient] final val _VERSION = "_version"
  private[esclient] final val _SCROLL_ID = "_scroll_id"
  private[esclient] final val MATCHED_QUERIES = "matched_queries"
  private[esclient] final val ERROR = "error"
  private[esclient] final val JEST_READ_TIMEOUT = 60000
  private[esclient] final val JEST_MAX_CONNS = 200
  //$COVERAGE-ON$

  val edgeNgramsAnalyzer = "edge_ngrams_analyzer"

  /** Factory method that returns a Jest Client
    * @param url url to add to client server list
    * @param maxConns maximum number of connections to support
    */
  def createJestClient(url: String, maxConns: Int = JEST_MAX_CONNS): JestClient = {
    val factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(url)
      .multiThreaded(true)
      .maxTotalConnection(maxConns)
      .defaultMaxTotalConnectionPerRoute(maxConns)
      .readTimeout(JEST_READ_TIMEOUT)
      .build())
    factory.getObject
  }

  /**
    * Returns an EsClient
    * @param url url for Jest client
    * @param maxConns maxConns for Jest client configuration
    */
  def createEsClient(url: String, maxConns: Int = JEST_MAX_CONNS): EsClient = {
    new EsClient(createJestClient(url, maxConns))
  }
}

/**
  * Elasticsearch Scala Client
  */
class EsClient(val jest: JestClient) extends JestUtils
  with EsClusterOps with EsIndexingMeta with EsIndexingDocs
  with EsScanAndScroll with EsSnapshots with EsAliases {
  def shutdownClient(): Unit = jest.shutdownClient()
}
