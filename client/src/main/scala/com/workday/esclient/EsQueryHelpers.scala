package com.workday.esclient

object EsQueryHelpers {

  def seqOption[T](els: Option[T]*): Option[Seq[T]] = {
    if (els.flatten.isEmpty) None else Some(els.flatten)
  }

  /**
    * Acceptable comparator key values for "terms": gte, gt, lte, lt
    */
  def range(field: String, terms: Map[String, Long]): Option[Map[String, Map[String, Any]]] = {
    Some(Map("range" -> Map(
      field -> terms
    )))
  }

  def term(
    field: String,
    ts: Any,
    options: Map[String, Any] = Map.empty
  ): Option[Map[String, Map[String, Any]]] = {
    Some(Map("term" -> (Map(field -> ts) ++ options)))
  }

  def terms(
    field: String,
    ts: Seq[Any],
    options: Map[String, Any] = Map.empty
  ): Option[Map[String, Map[String, Any]]] = {
    Some(Map("terms" -> (Map(field -> ts) ++ options)))
  }

  def aggregationTerms(
    field: String,
    options: Map[String, Any] = Map.empty
  ): Option[Map[String, Map[String, Any]]] = {
    Some(Map("terms" -> (Map("field" -> field) ++ options)))
  }

  def keyedAggregationRanges(field: String, ranges: Seq[Map[String, Any]]): Option[Map[String, Any]] = {
    val rangeMap = Map("field" -> field, "keyed" -> true, "ranges" -> ranges)
    Some(Map("range" -> rangeMap))
  }

  def keyedAggregationRange(rangeKey: String, from: Option[Double], to: Option[Double]): Option[Map[String, Any]] = {
    if (from.isDefined || to.isDefined) Some(Map("key" -> rangeKey) ++ from.map("from" -> _) ++ to.map("to" -> _))
    else None
  }

  def geoDistance(field: String, lat: String, lon: String, distance: String): Option[Map[String, Map[String, Object]]] = {
    Some(Map("geo_distance" -> Map("distance" -> distance, field -> Map("lat" -> lat, "lon" -> lon))))
  }

  /**
   * https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-terms-filter.html
   * Also see org.elasticsearch.index.query.TermsFilterParser to see ES's underlying code handling this.
   *
   * @param field the document field we are filtering on
   * @param index the index to lookup term values from
   * @param typeName the type of the lookup documents
   * @param path the field path in the lookup index to fetch term values from
   * @param shouldCacheLookup whether or not to cache the lookup of the terms from the index (caching the looked-up values)
   * @param shouldCacheFilter whether or not to cache the total filter (the docs matching the filter)
   * @param cacheKey the cache key to use for the total filter, useful if you want to manually wipe the cache
   * @param queryName name to give this query
   *                  (see [[https://www.elastic.co/guide/en/elasticsearch/reference/5.1/search-request-named-queries-and-filters.html]])
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

  def queryString(query: String, otherOption: (String, Any)*)
    : Option[Map[String, Map[String, Any]]] = {
    if (query.isEmpty)
      None
    else if (otherOption.isEmpty)
      Some(Map("query_string" -> Map("query" -> query)))
    else
      Some(Map("query_string" -> (otherOption :+ ("query" -> query)).toMap))
  }

  def matchQuery(field: String, str: String, otherOption: (String, Any)*): Option[Map[String, Map[String, Any]]] = {
    mkFieldQuery("match", field, str, otherOption)
  }

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

  def matchAll : Option[Map[String, Map[String, Any]]] = {
    Some(Map("match_all" -> Map()))
  }

  def matchPhrasePrefixQuery(
    field: String,
    str: String,
    otherOption: (String, Any)*
  ): Option[Map[String, Map[String, Any]]] = matchQuery(field, str, otherOption :+ ("type" -> "phrase_prefix"):_*)

  def prefixQuery(field: String, str: String): Option[Map[String, Map[String, Any]]] = {
    if (str.isEmpty)
      None
    else
      Some(Map("prefix" -> Map(field -> str)))
  }

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

  def bool(must: Option[_] = None, should: Option[_] = None, mustNot: Option[_] = None, options: Map[String, Any] = Map.empty): Option[Map[String, Any]] = {
    val query = Seq(must.map("must" -> _), should.map("should" -> _), mustNot.map("must_not" -> _)).flatten.toMap
    if (query.isEmpty)
      None
    else
      Some(Map("bool" -> (query ++ options)))
  }

  def orFilter(filters: Seq[Map[String, Any]]): Option[Map[String, Any]] = {
    Some(Map("or" -> filters))
  }

  def query(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("query", maps:_*)

  def filter(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("filter", maps:_*)

  def params(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("params", maps:_*)

  def aggs(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("aggs", maps:_*)

  def postFilter(maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = genMap("post_filter", maps:_*)

  private[this] def genMap(term: String, maps: Option[Map[String, Any]]*): Option[Map[String, Any]] = {
    seqOption(maps:_*).map(m => Map(term -> m.fold(Map())(_ ++ _)))
  }

  def filtered(qOpt: Option[Map[String, Any]], fOpt: Option[Map[String, Any]]): Option[Map[String, Any]] = {
    (qOpt, fOpt) match {
      case (None, None) => None
      case (None, Some(_)) => Some(Map("filtered" -> filter(fOpt).get))
      case (Some(_), None)  => qOpt
      case (Some(_), Some(_)) => Some(Map("filtered" -> (query(qOpt).get ++ filter(fOpt).get)))
    }
  }

  def functionScore(q: Option[Map[String, Any]], fs: Seq[Map[String, Any]], scoreMode: String): Option[Map[String, Any]] = {
    if (q.isEmpty)
      Some(Map("function_score" -> Map("functions" -> fs, "score_mode" -> scoreMode)))
    else
      Some(Map("function_score" -> Map("functions" -> fs, "score_mode" -> scoreMode, "query" -> q.get)))
  }

  def template(q: Option[Map[String, Any]], p: Option[Map[String, Any]]): Option[Map[String, Any]] = {
    if (q.isEmpty)
      None
    else
      Some(Map("template" -> (q.get ++ p.get)))
  }

  def disMax(queries: Seq[Map[String, Any]], tieBreaker: Option[Double] = None, boost: Option[Double] = None): Option[Map[String, Any]] = {
    Some(Map(
      "dis_max" -> Seq(
        Option("queries" -> queries),
        tieBreaker.map("tie_breaker" -> _),
        boost.map("boost" -> _)
      ).flatten.toMap
    ))
  }

  def range(fieldName: String, lowerBound: Option[Double], lowerBoundOp: ComparisonOp,
            upperBound: Option[Double], upperBoundOp: ComparisonOp): Option[Map[String, Any]] = {
    val range = Seq(lowerBound.map(lowerBoundOp.op -> _), upperBound.map(upperBoundOp.op -> _)).flatten.toMap
    if (range.isEmpty) None else Some(Map("range" -> Map(fieldName -> range)))
  }
}

// scalastyle:off
sealed trait ComparisonOp { def op: String }
case object gte extends ComparisonOp { val op = "gte" }
case object gt  extends ComparisonOp { val op = "gt" }
case object lte extends ComparisonOp { val op = "lte" }
case object lt  extends ComparisonOp { val op = "lt" }
//  scalastyle:on