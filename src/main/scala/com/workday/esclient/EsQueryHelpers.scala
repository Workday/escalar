/*
 * Copyright 2017 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */
// scalastyle:off number.of.methods

package com.workday.esclient

/**
  * Helper object for Elasticsearch queries.
  */
object EsQueryHelpers {

  /**
    * Returns a flattened sequence or None.
    * @param els Option of type T or None.
    * @tparam T generic type.
    * @return Option sequence of type T or None.
    */
  def seqOption[T](els: Option[T]*): Option[Seq[T]] = {
    if (els.flatten.isEmpty) None else Some(els.flatten)
  }

  /**
    * Returns a map for making Range Elasticsearch queries.
    * @param fieldName String field name to query on.
    * @param lowerBound Tuple of ComparisonOp for lower bound and Double value for lower bound of range.
    * @param upperBound Optional Tuple of ComparisonOp for upper bound and Double value for upper bound of range.
    * @tparam T Implicit Numeric.
    * @return Combined map for making Range queries.
    */
  def range[T: Numeric](fieldName: String, lowerBound: (ComparisonOp, T), upperBound: Option[(ComparisonOp, T)] = None): Option[Map[String, Any]] = {
    val range = Map(lowerBound._1.op -> lowerBound._2) ++ upperBound.map(bound => bound._1.op -> bound._2)
    Some(Map("range" -> Map(fieldName -> range)))
  }

  /**
    * Returns a Term map of a field key and values for the search Term Elasticsearch API.
    * @param field String field name for the term.
    * @param ts Any type value to associate with field.
    * @param options additional map of term options.
    * @return Map of term field key and any options.
    */
  def term(
    field: String,
    ts: Any,
    options: Map[String, Any] = Map.empty
  ): Option[Map[String, Map[String, Any]]] = {
    Some(Map("term" -> (Map(field -> ts) ++ options)))
  }

  /**
    * Returns a Terms map of a field key and a sequence of values for the search Terms Elasticsearch API.
    * @param field String field name for the term.
    * @param ts Sequence of values to associate with field.
    * @param options additional map of term options.
    * @return Map of terms field key and any options.
    */
  def terms(
    field: String,
    ts: Seq[Any],
    options: Map[String, Any] = Map.empty
  ): Option[Map[String, Map[String, Any]]] = {
    Some(Map("terms" -> (Map(field -> ts) ++ options)))
  }

  /**
    * Returns an Aggregations Terms map for the search Terms Aggregation Elasticsearch API.
    * @param field String aggregation field.
    * @param options Map of options to aggregate on.
    * @return Map of aggregation fields and any options.
    */
  def aggregationTerms(
    field: String,
    options: Map[String, Any] = Map.empty
  ): Option[Map[String, Map[String, Any]]] = {
    Some(Map("terms" -> (Map("field" -> field) ++ options)))
  }

  /**
    * Returns a keyed Aggregation Range map for the Range Aggregation Elasticsearch API.
    * Keyed parameter associates a unique string with each range bucket.
    * @param field String field to aggregate on.
    * @param ranges Sequences of map ranges to bucket.
    * @return Map of keyed range fields.
    */
  def keyedAggregationRanges(field: String, ranges: Seq[Map[String, Any]]): Option[Map[String, Any]] = {
    val rangeMap = Map("field" -> field, "keyed" -> true, "ranges" -> ranges)
    Some(Map("range" -> rangeMap))
  }

  /**
    * Returns a map of Range buckets with String keys for the Range Aggregation Elasticsearch API.
    * If neither a "to" nor "from" range are provided, returns None.
    * @param rangeKey String key for the range bucket.
    * @param from Double "from" range.
    * @param to Double "to" range.
    * @return Map of keyed range buckets.
    */
  def keyedAggregationRange(rangeKey: String, from: Option[Double], to: Option[Double]): Option[Map[String, Any]] = {
    if (from.isDefined || to.isDefined) Some(Map("key" -> rangeKey) ++ from.map("from" -> _) ++ to.map("to" -> _))
    else None
  }

  /**
    * Returns a map of Geo Distance parameters for the Geo Distance Elasticsearch API.
    * @param field String field to serve as the center location.
    * @param lat String latitude value.
    * @param lon String longitude value.
    * @param distance String distance value.
    * @return Map of Geo Distance parameters.
    */
  def geoDistance(field: String, lat: String, lon: String, distance: String): Option[Map[String, Map[String, Object]]] = {
    Some(Map("geo_distance" -> Map("distance" -> distance, field -> Map("lat" -> lat, "lon" -> lon))))
  }

  /**
   * Returns a map to run a terms lookup query using the Elasticsearch Terms lookup API.
   * Supports all but the routing field for the Terms lookup mechanism.
   * https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-terms-filter.html
   * Also see org.elasticsearch.index.query.TermsFilterParser to see ES's underlying code handling this.
   * @param field the document field we are filtering on.
   * @param index the index to lookup term values from.
   * @param typeName the type of the lookup documents.
   * @param path the field path in the lookup index to fetch term values from.
   * @param shouldCacheLookup whether or not to cache the lookup of the terms from the index (caching the looked-up values).
   * @param shouldCacheFilter whether or not to cache the total filter (the docs matching the filter).
   * @param cacheKey the cache key to use for the total filter, useful if you want to manually wipe the cache.
   * @param queryName name to give this query.
   *                  (see [[https://www.elastic.co/guide/en/elasticsearch/reference/5.1/search-request-named-queries-and-filters.html]])
   * @return Map for a Terms query with ES lookup fields.
   */
  // scalastyle:off parameter.number
  def termsFromLookup(
    field: String,
    index: String,
    typeName: String,
    id: String,
    path: String,
    shouldCacheLookup: Option[Boolean] = None,
    shouldCacheFilter: Option[Boolean] = None,
    cacheKey: Option[String] = None,
    queryName: Option[String] = None
  ): Option[Map[String, Map[String, Any]]] = {
  // scalastyle:on parameter.number
    val valuesIndexMap = Map("index" -> index, "type" -> typeName, "id" -> id, "path" -> path) ++ shouldCacheLookup.map("cache" -> _).toMap
    val map: Map[String, Any] = Map(
      field -> valuesIndexMap) ++
      shouldCacheFilter.map("_cache" -> _).toMap ++
      cacheKey.map("_cache_key" -> _).toMap ++
      queryName.map("_name" -> _).toMap
    Some(Map("terms" -> map))
  }

  /**
    * Returns a map with a query string for use in the Query Elasticsearch API.
    * @param query String query for ES.
    * @param otherOption Tuples of additional parameters to include in ES query.
    * @return Query string map for ES.
    */
  def queryString(query: String, otherOption: (String, Any)*)
    : Option[Map[String, Map[String, Any]]] = {
    if (query.isEmpty)
      None
    else if (otherOption.isEmpty)
      Some(Map("query_string" -> Map("query" -> query)))
    else
      Some(Map("query_string" -> (otherOption :+ ("query" -> query)).toMap))
  }

  /**
    * Returns a map for making match queries using the Match Elasticsearch API.
    * @param field String field to match.
    * @param str String value for field.
    * @param otherOption Tuples of additional parameters to include in the query.
    * @return Map for using the Query Match API.
    */
  def matchQuery(field: String, str: String, otherOption: (String, Any)*): Option[Map[String, Map[String, Any]]] = {
    mkFieldQuery("match", field, str, otherOption)
  }

  /**
    * Returns a map for making mutli match queries using the Multi Match Elasticsearch API.
    * @param fields Sequence of String fields to match.
    * @param str String value for field.
    * @param otherOption Tuples of additional parameters to include in the query.
    * @return Map for using the Query Multi Match API.
    */
  def multiMatchQuery(
    fields: Seq[String],
    str: String,
    otherOption: (String, Any)*): Option[Map[String, Map[String, Any]]] = {
    Some(Map("multi_match" -> (Map(
        "query" -> str,
        "fields" -> fields
      ) ++ otherOption.toMap)
    ))
  }

  /**
    * Returns a map for making Match All queries using the Match All Elasticsearch API.
    * @return Map for using the Query Match All API.
    */
  def matchAll : Option[Map[String, Map[String, Any]]] = {
    Some(Map("match_all" -> Map()))
  }

  /**
    * Returns a map for making Match queries with the phrase_prefix option.
    * @param field String field to match.
    * @param str String value for field.
    * @param otherOption Tuples of additional parameters to include in the query.
    * @return Map for using the Query Match API with phrase_prefix matching.
    */
  def matchPhrasePrefixQuery(
    field: String,
    str: String,
    otherOption: (String, Any)*
  ): Option[Map[String, Map[String, Any]]] = matchQuery(field, str, otherOption :+ ("type" -> "phrase_prefix"):_*)

  /**
    * Returns a map for making Prefix queries using the Prefix Elasticsearch API.
    * @param field String field to match.
    * @param str String value for field.
    * @return Map for using the Query Prefix API.
    */
  def prefixQuery(field: String, str: String): Option[Map[String, Map[String, Any]]] = {
    if (str.isEmpty)
      None
    else
      Some(Map("prefix" -> Map(field -> str)))
  }

  /**
    * Constructs a basic map that can be used for different types of Elasticsearch queries.
    * @param queryType String type of query used.
    * @param field String field to restrict search to.
    * @param str String value for field.
    * @param otherOption Tuples of additional query parameters.
    * @param queryKey String query key for ES request.
    * @return Map for making ES queries.
    */
  private def mkFieldQuery(
    queryType: String,
    field: String,
    str: String,
    otherOption: Seq[(String, Any)],
    queryKey: String = "query"
  ): Option[Map[String, Map[String, Any]]] = {
    if (str.isEmpty)
      None
    else if (otherOption.isEmpty)
      Some(Map(queryType -> Map(field -> str)))
    else
      Some(Map(
        queryType -> Map(
          field -> (otherOption :+ (queryKey -> str)).toMap
        )
      ))
  }

  /**
    * Returns a map for making Boosting queries using the Boosting Elasticsearch API.
    * @param positive Map containing the field and value to positively boost.
    * @param negative Map containing the field and value to negatively boost.
    * @param negativeBoost Value for negative boosting, set to 0.2.
    * @return Map for using the Query Boosting API.
    */
  def boosting(positive: Option[_] = None, negative: Option[_] = None, negativeBoost: Option[Double] = Some(0.2)): Option[Map[String, Any]] = {
    if (positive.isEmpty && negative.isEmpty)
      None
    else
      Some(Map("boosting" -> Map(
        "positive" -> positive.getOrElse(Map()),
        "negative" -> negative.getOrElse(Map()),
        "negative_boost" -> negativeBoost.getOrElse(0.2)
      )))
  }

  /**
    * Returns a map for making Bool queries using the Bool Elasticsearch API.
    * @param must Map or Seq of fields and values that must be matched.
    * @param should Map or Seq of fields and values that should be matched. Threshold set by minimum_should_match parameter.
    * @param mustNot Map or Seq of fields and values.
    * @param options Additional fields to include in Bool query, like minimum_should_match.
    * @return Map for using the Query Bool API.
    */
  def bool(must: Option[_] = None, should: Option[_] = None, mustNot: Option[_] = None, options: Map[String, Any] = Map.empty): Option[Map[String, Any]] = {
    val query = Seq(must.map("must" -> _), should.map("should" -> _), mustNot.map("must_not" -> _)).flatten.toMap
    if (query.isEmpty)
      None
    else
      Some(Map("bool" -> (query ++ options)))
  }

  /**
    * Returns a map of filters for making Or queries using the Or Filter Elasticsearch API.
    * @param filters Sequence of fields and values to filter on.
    * @return Map of filter for using the Or Filter API.
    */
  def orFilter(filters: Seq[Map[String, Any]]): Option[Map[String, Any]] = {
    Some(Map("or" -> filters))
  }

  /**
    * Returns a map for making a general Elasticsearch query.
    * @param maps for being included in the ES query.
    * @return a single Map with a top level "query" key.
    */
  def query(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("query", maps:_*)

  /**
    * Returns a map for using Elasticsearch's filter mechanism.
    * @param maps for being included in the ES query.
    * @return a single Map with a top level "filter" key.
    */
  def filter(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("filter", maps:_*)

  /**
    * Returns a map for including a group of parameters in an Elasticsearch request.
    * @param maps for being included in the ES query.
    * @return a single Map with a top level "params" key.
    */
  def params(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("params", maps:_*)

  /**
    * Returns a map for using Elasticsearch's aggs mechanism.
    * @param maps for being included in the ES query.
    * @return a single Map with a top level "aggs" key.
    */
  def aggs(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("aggs", maps:_*)

  /**
    * Returns a map for using Elasticsearch's post_filter mechanism.
    * @param maps for being included in the ES query.
    * @return a single Map with a top level "post_filter" key.
    */
  def postFilter(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("post_filter", maps:_*)

  /**
    * Maps a top level term to a Map[String,Any] containing Elasticsearch query parameters.
    * @param term String term to set as parent key.
    * @param maps a number of maps containing ES parameters.
    * @return a single Map with a top level term and associated maps.
    */
  private[this] def genMap(term: String, maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = {
    seqOption(maps:_*).map(m => Map(term -> m.fold(Map())(_ ++ _)))
  }

  /**
    * Returns a map for making filtered Elasticsearch queries.
    * Combines and matches on optional query ops and filter ops.
    * @param qOpt Map representing the query operation.
    * @param fOpt Map representing the filter operation.
    * @return Combined map for making filtered queries.
    */
  def filtered(qOpt: Option[Map[String, Any]], fOpt: Option[Map[String, Any]]): Option[Map[String, Any]] = {
    (qOpt, fOpt) match {
      case (None, None) => None
      case (None, Some(_)) => Some(Map("filtered" -> filter(fOpt).get))
      case (Some(_), None)  => qOpt
      case (Some(_), Some(_)) => Some(Map("filtered" -> (query(qOpt).get ++ filter(fOpt).get)))
    }
  }

  /**
    * Returns a map for making Function Score Elastichsearch queries.
    * This API requires the user to provide functions to score each document returned by the query.
    * @param q Map representing the query operation.
    * @param fs Map representing the function score fields.
    * @param scoreMode String representing how the new scores will be combined.
    * @return Combined map for making Function Score queries.
    */
  def functionScore(q: Option[Map[String, Any]], fs: Seq[Map[String, Any]], scoreMode: String): Option[Map[String, Any]] = {
    if (q.isEmpty)
      Some(Map("function_score" -> Map("functions" -> fs, "score_mode" -> scoreMode)))
    else
      Some(Map("function_score" -> Map("functions" -> fs, "score_mode" -> scoreMode, "query" -> q.get)))
  }

  /**
    * Generates sort section of query
    * @param sortFields a sequence of tuples (fieldName: String, descending: Boolean).
    * @return Map for making Sort queries.
    */
  def sort(sortFields: Seq[Map[String, Any]]): Option[Map[String, Any]] = {
      Some(Map("sort" -> sortFields))
  }

  /**
    * Returns a template map for making Template Elastichsearch queries.
    * ES Template queries use params maps to substitute into the templated query.
    * @param q Map representing the templated query operation.
    * @param p Map representing the params to substitute into the query.
    * @return Combined map for making Template queries.
    */
  def template(q: Option[Map[String, Any]], p: Option[Map[String, Any]]): Option[Map[String, Any]] = {
    if (q.isEmpty)
      None
    else
      Some(Map("template" -> (q.get ++ p.get)))
  }

  /**
    * Returns a map for making Dis Max Elasticsearch queries.
    * Dis Max returns the union of of documents retrieved from the subqueries.
    * @param queries Map representing subqueries.
    * @param tieBreaker Double tiebreaker parameter for favoring documents with preferred term usage.
    * @param boost Double boost parameter.
    * @return Combined map for making Dis Max queries.
    */
  def disMax(queries: Seq[Map[String, Any]], tieBreaker: Option[Double] = None, boost: Option[Double] = None): Option[Map[String, Any]] = {
    Some(Map(
      "dis_max" -> Seq(
        Option("queries" -> queries),
        tieBreaker.map("tie_breaker" -> _),
        boost.map("boost" -> _)
      ).flatten.toMap
    ))
  }
}

// scalastyle:on
// scalastyle:off
/**
  * Convenience trait for representing Elasticsearch comparison operators.
  */
sealed trait ComparisonOp { def op: String }

/**
  * Case for the "greater than or equal to" comparison operator.
  */
case object gte extends ComparisonOp { val op = "gte" }

/**
  * Case for the "greater than" comparison operator.
  */
case object gt  extends ComparisonOp { val op = "gt" }

/**
  * Case for the "less than or equal to" operator.
  */
case object lte extends ComparisonOp { val op = "lte" }

/**
  * Case for the "less than" operator.
  */
case object lt  extends ComparisonOp { val op = "lt" }
//  scalastyle:on