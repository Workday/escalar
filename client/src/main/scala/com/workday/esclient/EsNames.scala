package com.workday.esclient

object EsNames {
  val FilterPath = "filter_path"
  // Values for ES to return in its result set; we dont include hits.hits because it includes all values in the hits array
  // As a side effect though, if no hits are returned, then hits.hits doesn't exist instead of being an empty array
  private[this] val WhitelistedParams = Seq(
    "_scroll_id", "took", "timed_out",
    "hits.total", "hits.max_score",
    "hits.hits._id", "hits.hits._score", "hits.hits.matched_queries",
    "aggregations"
  )
  val FilterPathParams = WhitelistedParams.mkString(",")
  val standardAnalyzerName = "standard"
  // Number of documents to return on each page of scan and scroll searches
  val EsScanAndScrollSize = 100
  // Amount of time to keep the scroll snapshot (how long we can scan through it, https://www.elastic.co/guide/en/elasticsearch/guide/current/scan-scroll.html)
  val EsScanAndScrollTime = "1m"
  val NAME_QUERY_DELIMITER = "##"
  val DOCS_SUFFIX = "docs"
  val NAME_DELIMITER = "@"
  // don't store strings greater than the length below for the raw field in the "other" dynamic mapping
  val OtherMappingRawFieldIgnoreAboveLength = 1000
}
