/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient

import com.google.common.annotations.VisibleForTesting
import com.google.gson.JsonObject
import io.searchbox.client.JestResult
import io.searchbox.core.{Search, SearchScroll}
import io.searchbox.params.Parameters

/**
  * Trait wrapping Elasticsearch Scan and Scroll APIs.
  */
trait EsScanAndScroll extends JestUtils with EsQuery {
  private[this] val emptyScanAndScrollResponse: EsResult[ScanAndScrollResponse] =
    EsResponse(ScanAndScrollResponse(0, SearchHits(0, None, Seq.empty), new JsonObject, ""))
  private[this] val UnlimitedResultsDefaultParams = Map(EsNames.FilterPath -> EsNames.FilterPathParams)

  /**
    * Performs a ScanAndScroll search, combines all paged results into an iterator and
    * aggregates each result in the iterator into a single EsResult[EsSearchResponse].
    * @param index String index to search
    * @param typeName String index type
    * @param query String query to perform
    * @param scanAndScrollSize Int size of each scan and scroll response
    * @return EsResult[EsSearchResponse] of aggregate result from scan-and-scrolled results
    */
  def unlimitedSearch(index: String, typeName: String = "", query: String, scanAndScrollSize: Int): EsResult[EsSearchResponse] = {
    val unlimitedResultsParams = UnlimitedResultsDefaultParams ++ Map(Parameters.SIZE -> scanAndScrollSize)
    val scanAndScrollSearch = createScrolledSearch(index, typeName, query, unlimitedResultsParams)
    val scanAndScrollIter = getScrolledSearchIterator(scanAndScrollSearch, true)
    val scanAndScrollResult = scanAndScrollIter.foldLeft(emptyScanAndScrollResponse)(accumulateScanAndScrollResponses)

    // Turn ScanAndScroll result into an EsResult
    scanAndScrollResult match {
      case EsResponse(response) => EsResponse(EsSearchResponse(response.took, response.hits, response.aggregations))
      case e: EsError => e
      case e: EsInvalidResponse => e
    }
  }

  /**
    * Does a scan and scroll search and returns an iterator over the results.
    * (https://www.elastic.co/guide/en/elasticsearch/guide/current/scan-scroll.html)
    * @param index String index to search
    * @param typeName String index type
    * @param query String query to perform
    * @param params Map[String,Any] query parameters to include; defaults to empty Map
    * @return Iterator over EsResults of SearchHits
    */
  def createScrolledSearchIterator(index: String, typeName: String, query: String, params: Map[String, Any] = Map()): Iterator[EsResult[SearchHits]] = {
    val result = createScrolledSearch(index, typeName, query, params)
    getScrolledSearchHitsIterator(result)
  }

  /**
    * Creates a scrolled search and returns the Jest response.
    * @param index String index to search on
    * @param typeName String type name to search
    * @param query String query to search on
    * @param params String params for search query
    * @return EsResult of Elasticsearch scan and scroll response
    */
  def createScrolledSearch(index: String, typeName: String = "", query: String, params: Map[String, Any] = Map()): EsResult[ScanAndScrollResponse] = {
    val jestResult : JestResult = jest.execute(buildScrolledSearchAction(query, index, typeName, params))
    handleScrollResult(jestResult)
  }

  /**
    * Gets the scrolled search result using a scroll ID
    * @param scrollID String provided scroll ID
    * @return EsResult of Elasticsearch scan and scroll response
    */
  def getScrolledSearchResults(scrollID: String): EsResult[ScanAndScrollResponse] = {
    val jestResult : JestResult = jest.execute(buildGetScrolledSearchResultsAction(scrollID))
    handleScrollResult(jestResult)
  }

  /**
    * Returns the Elasticsearch scan and scroll response from the Jest client
    * @param scrollResult Jest result to be handled
    * @return EsResult of Elasticsearch scan and scroll response
    */
  private[this] def handleScrollResult(scrollResult: JestResult): EsResult[ScanAndScrollResponse] = {
    handleJestResult(scrollResult) { successfulJestResult =>
      val json = successfulJestResult.getJsonObject
      val aggregations = Option(json.get("aggregations")).getOrElse(new JsonObject)
      ScanAndScrollResponse(json.get("took").getAsLong, handleHitsInResult(json), aggregations.getAsJsonObject, json.get(EsClient._SCROLL_ID).getAsString())
    }
  }

  /**
    * Adds the values of the nextScanAndScrollResult onto the accumulator.
    * If we encounter an error in the scan and scroll search, just return the error and stop accumulating.
    * @param scanAndScrollResultAccumulator tuple of current accumulated results and boolean of whether an error has been encountered already
    * @param nextScanAndScrollResult the next "page" in the ScanAndScroll search result
    * @return the current accumulated response and whether we should continue accumulating
    */
  @VisibleForTesting
  private[workday] def accumulateScanAndScrollResponses(scanAndScrollResultAccumulator: EsResult[ScanAndScrollResponse],
    nextScanAndScrollResult: EsResult[ScanAndScrollResponse]): EsResult[ScanAndScrollResponse] = {

    (scanAndScrollResultAccumulator, nextScanAndScrollResult) match {
      case (EsResponse(response1), EsResponse(response2)) =>
        EsResponse(ScanAndScrollResponse(
          response1.took + response2.took,
          addSearchHits(response1.hits, response2.hits),
          // Take the first non-null aggregation of the responses. Theoretically, only the first page will have an aggregation:
          // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
          if (response1.aggregations.entrySet().isEmpty) response2.aggregations else response1.aggregations,
          response1.scrollID
        ))
      case (EsResponse(response), e) => e
      case (e, _) => e
    }
  }

  /**
    * Returns a search hits iterator from the result of a scan and scroll search.
    * @param result EsResult scan and scroll response of the current "page" in the scroll result
    * @return iterator SearchHits
    */
  @VisibleForTesting
  private[workday] def getScrolledSearchHitsIterator(result: EsResult[ScanAndScrollResponse]): Iterator[EsResult[SearchHits]] = {
    // Aggregate an iterator of ScanAndScrollResponses
    val scanAndScrollIter = getScrolledSearchIterator(result)
    // extract the search hits from the ScanAndScrollResponse iterator
    scanAndScrollIter.map {
      case EsResponse(scanAndScrollResponse) => EsResponse(scanAndScrollResponse.hits)
      case e: EsError => e
      case e: EsInvalidResponse => e
    }
  }

  /**
    * Recursively builds a scan and response iterator starting at current page of the results.
    * @param resultPage EsResult scan and scroll response of the current "page" in the scroll result
    * @param forceFetchNextResults Boolean the initial createScrolledSearch() call returns a scrollID with no hits, so in that
    *                        case only we have to force it to fetch the next results
    * @return an iterator of ScanAndScrollResponses
    */
  @VisibleForTesting
  private[workday] def getScrolledSearchIterator(
    resultPage: EsResult[ScanAndScrollResponse],
    forceFetchNextResults: Boolean = true): Iterator[EsResult[ScanAndScrollResponse]] = {
    resultPage match {
      case EsResponse(response) =>
        Iterator(EsResponse(response)) ++ {
          if (forceFetchNextResults || response.hits.hits.nonEmpty) {
            val nextResult = getScrolledSearchResults(response.scrollID)
            getScrolledSearchIterator(nextResult, false)
          } else {
            Iterator.empty
          }
        }
      case e: EsError => Iterator(e)
      case e: EsInvalidResponse => Iterator(e)
    }
  }

  /**
    * Combines two SearchHits into a new SearchHit.
    * The maximum of two maxScores is preserved.
    * @param searchHits1 first SearchHits instance
    * @param searchHits2 second SearchHits instance
    * @return the combined SearchHits instance.
    */
  private[this] def addSearchHits(searchHits1: SearchHits, searchHits2: SearchHits): SearchHits = {
    SearchHits(
      Math.max(searchHits1.total, searchHits2.total),
      maxMaxScores(searchHits1.maxScore, searchHits2.maxScore),
      searchHits1.hits ++ searchHits2.hits
    )
  }

  /**
    * Returns the max of two maxScore Options.
    * Finds the first non-None maxScore; if both are None, return None.
    * @param maxScore1 Option of Double or None
    * @param maxScore2 Option of Double or None
    * @return Option max score of Double or None
    */
  private[this] def maxMaxScores(maxScore1: Option[Double], maxScore2: Option[Double]): Option[Double] = {
    (maxScore1, maxScore2) match {
      case (Some(ms1), Some(ms2)) => Some(ms1 max ms2)
      case (ms1, ms2) => ms1 orElse ms2
    }
  }

  /**
    * Returns a search action set with given parameters.
    * @param query String query to be made
    * @param index String index to query on
    * @param typeName String typename to search
    * @param params Map of params to set
    * @return Elasticsearch search action
    */
  @VisibleForTesting
  private[esclient] def buildScrolledSearchAction(query: String, index: String, typeName: String = "", params: Map[String, Any] = Map()): Search = {
    var searchAction = new Search.Builder(query).addIndex(index)
      .setParameter(Parameters.SCROLL, EsNames.EsScanAndScrollTime)
      .setParameter(Parameters.SIZE, EsNames.EsScanAndScrollSize)
    if (typeName.nonEmpty) searchAction = searchAction.addType(typeName)
    params.foreach { case (key: String, value: Any) => searchAction.setParameter(key, value) }
    searchAction.build()
  }

  /**
    * Builds a SearchScroll instance with given scrollID.
    * @param scrollID String ID
    * @return SearchScroll instance
    */
  @VisibleForTesting
  private[esclient] def buildGetScrolledSearchResultsAction(scrollID: String): SearchScroll = {
    new SearchScroll.Builder(scrollID, EsNames.EsScanAndScrollTime).build()
  }
}
