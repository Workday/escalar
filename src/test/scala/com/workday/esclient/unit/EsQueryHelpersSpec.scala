package com.workday.esclient.unit

import com.workday.esclient.{EsQueryHelpers, gte, lt, gt, lte}

class EsQueryHelpersSpec extends org.scalatest.FlatSpec with org.scalatest.Matchers with org.scalatest.BeforeAndAfterAll
  with org.scalatest.BeforeAndAfterEach with org.scalatest.mock.MockitoSugar  {
  private val testField = "test"
  private val testParam = "test_param"
  private val prefixOption = ("type" -> "phrase_prefix")
  "#setOption" should "convert a Sequence of Options to a Option of Sequence" in {
    EsQueryHelpers.seqOption(Some("qwe"), None, Some(1), Some(22), None) shouldBe Some(Seq("qwe", 1, 22))
  }

  "#setOption" should "convert a Sequence of Nones to a None" in {
    EsQueryHelpers.seqOption(None, None) shouldBe None
  }

  "#term" should "return Some map when empty any is provided" in {
    EsQueryHelpers.term(testField, None) shouldBe Some(Map("term" -> Map(testField -> None)))
  }

  "#term" should "return Some map when non empty string is provided" in {
    EsQueryHelpers.term(testField, "2") shouldBe Some(Map("term" -> Map(testField -> "2")))
  }

  "#term" should "return Some map when non empty string is provided with options" in {
    EsQueryHelpers.term(testField, "2", Map("_name" -> "test")) shouldBe Some(Map("term" -> Map(testField -> "2", "_name" -> "test")))
  }

  "#terms" should "return Some map on empty sequence" in {
    EsQueryHelpers.terms(testField, Seq()) shouldBe Some(Map("terms" -> Map("test" -> Nil)))
  }

  "#terms" should "return Some map with proper values" in {
    EsQueryHelpers.terms(testField, Seq(123, "222")) shouldBe Some(Map("terms" -> Map(testField -> Seq(123, "222"))))
  }

  "#terms" should "return Some map with proper values with options" in {
    EsQueryHelpers.terms(testField, Seq(123, "222", 11), Map("option" -> "optionValue")) shouldBe Some(Map("terms" -> Map(testField -> Seq(123, "222", 11), "option" -> "optionValue")))
  }

  "#range" should "return Some map with proper values with appropriate range terms" in {
    EsQueryHelpers.range(testField, Map("range1" -> 1l, "range2" -> 2l)) shouldBe Some(Map("range" -> Map(testField -> Map("range1" -> 1, "range2" -> 2))))
  }

  "#termsFromLookup" should "return Some map with lookup values" in {
    EsQueryHelpers.termsFromLookup(testField, "lookupIndex", "lookupType", "lookupId", "lookupField", Some(false), Some(true), Some("cacheKey"), Some("queryName")) shouldBe
      Some(Map("terms" ->
        Map(
          testField -> Map("index" -> "lookupIndex", "type" -> "lookupType", "id" -> "lookupId", "path" -> "lookupField", "cache" -> false),
          "_cache" -> true,
          "_cache_key" -> "cacheKey",
          "_name" -> "queryName")
      ))
  }

  "#matchQuery" should "return none if string is empty" in {
    EsQueryHelpers.matchQuery(testField, "") shouldBe None
  }

  "#matchQuery" should "return Some map with no otherOptions" in {
    EsQueryHelpers.matchQuery(testField, "abc") shouldBe Some(Map("match" -> Map(testField -> "abc")))
  }

  "#matchQuery" should "return Some map with otherOptions" in {
    EsQueryHelpers.matchQuery(testField, "abc", "operator" -> "and", "operator2" -> "and2") shouldBe Some(Map("match" -> Map(testField -> Map("query" ->
      "abc", "operator" -> "and", "operator2" -> "and2"))))
  }

  "#multiMatchQuery" should "return Some map with otherOptions" in {
    val fieldsSequence = Seq("field1", "field2")
    val queryString = "abc"
    val multiMatch = "multi_match"
    val operatorAnd = ("operator" -> "and")
    val typePhrase = ("type" -> "phrase")

    EsQueryHelpers.multiMatchQuery(fieldsSequence, queryString, operatorAnd, typePhrase) shouldBe
      Some(Map(multiMatch -> Map(
        "query" -> queryString,
        "fields" -> fieldsSequence, operatorAnd, typePhrase)))
  }


  "#matchAll" should "return Some map" in {
    EsQueryHelpers.matchAll shouldBe Some(Map("match_all" -> Map()))
  }

  "#matchPhrasePrefixQuery" should "return Some map with no prefix option" in {
    EsQueryHelpers.matchPhrasePrefixQuery(testField, "abc") shouldBe Some(Map("match" -> Map(testField -> Map("query" -> "abc", prefixOption))))
  }

  "#prefixQuery" should "return simple prefix query" in {
    EsQueryHelpers.prefixQuery(testField, "abc") shouldBe Some(Map("prefix" -> Map(testField -> "abc")))
  }

  "#prefixQuery" should "return None when no search term is provided" in {
    EsQueryHelpers.prefixQuery(testField, "") shouldBe None
  }

  "#matchPhrasePrefixQuery" should "return Some map with otherOptions" in {
    EsQueryHelpers.matchPhrasePrefixQuery(testField, "abc", "operator" -> "and", "operator2" -> "and2") shouldBe Some(Map("match" -> Map(testField -> Map("query" ->
      "abc", "operator" -> "and", "operator2" -> "and2", prefixOption))))
  }

  "#queryString" should "return Some map with query and no option" in {
    EsQueryHelpers.queryString("aaa AND bbb") shouldBe Some(Map("query_string" -> Map("query" -> "aaa AND bbb")))
  }

  "#queryString" should "return Some map with query and otherOptions" in {
    EsQueryHelpers.queryString("aaa AND bbb", "option1" -> "a", "option2" -> "b") shouldBe
        Some(Map("query_string" -> Map("option1" -> "a", "option2" -> "b", "query" -> "aaa AND bbb")))
  }

  "#queryString" should "return None when there is no query" in {
    EsQueryHelpers.queryString("", "option" -> "x") shouldBe None
  }

  "#boosting" should "return none if both positive and negative is None" in {
    EsQueryHelpers.boosting(None, None, Some(0.3)) shouldBe None
  }

  "#boosting" should "return Some map with given positive and default negative and negative boosting" in {
    EsQueryHelpers.boosting(Some(123)) shouldBe Some(Map("boosting" -> Map(
      "positive" -> 123,
      "negative" -> Map(),
      "negative_boost" -> 0.2
    )))
  }

  "#boosting" should "return Some map with given negative and negative boosting and default positive" in {
    EsQueryHelpers.boosting(negative = Some(123), negativeBoost = Some(0.3)) shouldBe Some(Map("boosting" -> Map(
      "positive" -> Map(),
      "negative" -> 123,
      "negative_boost" -> 0.3
    )))
  }

  "#bool" should "return none if must, should and must not are empty" in {
    EsQueryHelpers.bool(must = None, should = None, mustNot = None) shouldBe None
  }

  "#bool" should "return Some map of any and all are filled" in {
    EsQueryHelpers.bool(must = Some(123)) shouldBe Some(Map("bool" -> Map("must" -> 123)))
    EsQueryHelpers.bool(should = Some(123)) shouldBe Some(Map("bool" -> Map("should" -> 123)))
    EsQueryHelpers.bool(mustNot = Some(123)) shouldBe Some(Map("bool" -> Map("must_not" -> 123)))
    EsQueryHelpers.bool(must = Some(222), should = Some(555), mustNot = Some(123)) shouldBe Some(Map("bool" -> Map("must_not" -> 123, "should" -> 555,
      "must" -> 222)))
  }

  "#bool" should "return Some map of any with options" in {
    EsQueryHelpers.bool(should = Some(123), options = Map("_name" -> "test")) shouldBe Some(Map("bool" -> Map("should" -> 123, "_name" -> "test")))
  }

  "#query" should "return Some empty map" in {
    EsQueryHelpers.query() shouldBe None
  }

  "#query" should "return Some map that combines queries" in {
    EsQueryHelpers.query(Some(Map("abc" -> 123, "aaa" -> 222)), None, Some(Map("ddd" -> 2223, "abc" -> 232))) shouldBe
      Some(Map("query" -> Map("abc" -> 232, "aaa" -> 222, "ddd" -> 2223)))
  }

  "#params" should "return Some empty map" in {
    EsQueryHelpers.params() shouldBe None
  }

  "#params" should "return Some map that combines params" in {
    EsQueryHelpers.params(Some(Map("abc" -> 123, "aaa" -> 222)), None, Some(Map("ddd" -> 2223, "abc" -> 232))) shouldBe
      Some(Map("params" -> Map("abc" -> 232, "aaa" -> 222, "ddd" -> 2223)))
  }

  "#filter" should "return Some empty map" in {
    EsQueryHelpers.filter() shouldBe None
  }

  "#filter" should "return Some map that combines filters" in {
    EsQueryHelpers.filter(Some(Map("abc" -> 123, "aaa" -> 222)), None, Some(Map("ddd" -> 2223, "abc" -> 232))) shouldBe
      Some(Map("filter" -> Map("abc" -> 232, "aaa" -> 222, "ddd" -> 2223)))
  }

  "#aggs" should "return Some empty map" in {
    EsQueryHelpers.aggs() shouldBe None
  }

  "#aggs" should "return Some map that combines aggregations" in {
    EsQueryHelpers.aggs(Some(Map("abc" -> 123, "aaa" -> 222)), None, Some(Map("ddd" -> 2223, "abc" -> 232))) shouldBe
      Some(Map("aggs" -> Map("abc" -> 232, "aaa" -> 222, "ddd" -> 2223)))
  }

  "#postFilter" should "return Some empty map" in {
    EsQueryHelpers.postFilter() shouldBe None
  }

  "#postFilter" should "return Some map that combines post filters" in {
    EsQueryHelpers.postFilter(Some(Map("abc" -> 123, "aaa" -> 222)), None, Some(Map("ddd" -> 2223, "abc" -> 232))) shouldBe
      Some(Map("post_filter" -> Map("abc" -> 232, "aaa" -> 222, "ddd" -> 2223)))
  }

  "#filtered" should "return None when query and filter are absent" in {
    EsQueryHelpers.filtered(None, None) shouldBe None
  }

  "#filtered" should "return Some merged map of filters and queries" in {
    EsQueryHelpers.filtered(Some(Map("abc" -> 123, "aaa" -> 222)), Some(Map("ddd" -> 2223, "abc" -> 232))) shouldBe
      Some(Map("filtered" -> Map("query" -> Map("abc" -> 123, "aaa" -> 222), "filter" -> Map("abc" -> 232, "ddd" -> 2223))))
  }

  "#filtered" should "return filters when query not present" in {
    EsQueryHelpers.filtered(None, Some(Map("ddd" -> 2223, "abc" -> 232))) shouldBe
      Some(Map("filtered" -> Map("filter" -> Map("abc" -> 232, "ddd" -> 2223))))
  }

  "#filtered" should "return work when query is present but filter is not" in {
    EsQueryHelpers.filtered(Some(Map("abc" -> 123, "aaa" -> 222)), None) shouldBe
      Some(Map("abc" -> 123, "aaa" -> 222))
  }

  "#functionScore" should "return valid function_score clause with query when query present" in {
    EsQueryHelpers.functionScore(Some(Map("hi" -> 123)), Seq(Map("ddd" -> 2223), Map("abc" -> 232)), "mode") shouldBe
      Some(Map("function_score" -> Map("functions" -> Seq(Map("ddd" -> 2223), Map("abc" -> 232)), "score_mode" -> "mode", "query" -> Map("hi" -> 123))))
  }

  "#functionScore" should "return function_score clause without query when query parameter is None" in {
    EsQueryHelpers.functionScore(None, Seq(Map("ddd" -> 2223), Map("abc" -> 232)), "mode") shouldBe
      Some(Map("function_score" -> Map("functions" -> Seq(Map("ddd" -> 2223), Map("abc" -> 232)), "score_mode" -> "mode")))
  }

  "sort" should "return a sort Map with order correctly specified" in {
    EsQueryHelpers.sort(Seq(Map("field" -> Map("order" -> "asc")))) shouldBe
      Some(Map("sort" -> Seq(Map("field" -> Map("order" -> "asc")))))

    EsQueryHelpers.sort(Seq(Map("field" -> Map("order" -> "desc")))) shouldBe
      Some(Map("sort" -> Seq(Map("field" -> Map("order" -> "desc")))))
  }

  "sort" should "return a sort Map with order correctly specified with multiple fields" in {
    EsQueryHelpers.sort(Seq(Map("field" -> Map("order" -> "asc")), Map("field2" -> Map("order" -> "desc")))) shouldBe
      Some(Map("sort" -> Seq(Map("field" -> Map("order" -> "asc")),
        Map("field2" -> Map("order" -> "desc"))
      )))
  }

  "#template" should "return Some template value when query is present" in {
    EsQueryHelpers.template(Some(Map("abc" -> 123)), Some(Map("ddd" -> 456))) shouldBe
      Some(Map("template" -> Map("abc" -> 123, "ddd" -> 456)))
  }

  "#template" should "return None when query not present" in {
    EsQueryHelpers.template(None, Some(Map("ddd" -> 2223, "abc" -> 232))) shouldBe None
  }

  "#disMax" should "return Some dismax query" in {
    EsQueryHelpers.disMax(Seq(Map("abc" -> 123, "aaa" -> 222), Map("ddd" -> 2223, "abc" -> 232)), tieBreaker = Some(2), boost = Some(25)) shouldBe
      Some(Map("dis_max" -> Map("queries" -> Seq(Map("abc" -> 123, "aaa" -> 222), Map("ddd" -> 2223, "abc" -> 232)), "tie_breaker" -> 2.0, "boost" -> 25.0)))
  }

  "#orFilter" should "return Some map if no filters are provided" in {
    EsQueryHelpers.orFilter(Nil) shouldBe Some(Map("or" -> Nil))
  }

  "#orFilter" should "return Some map if filters are provided" in {
    EsQueryHelpers.orFilter(Seq(Map("filter1"->"foo"), Map("filter2" -> "bar"))) shouldBe Some(Map("or" -> Seq(Map("filter1"->"foo"), Map("filter2" -> "bar"))))
  }

  "#aggsTerms" should "return Some map for aggregating terms" in {
    EsQueryHelpers.aggregationTerms("term_field") shouldBe Some(Map("terms" -> Map("field" -> "term_field")))
  }

  "#aggsTerms" should "return Some map for aggregating terms with options" in {
    EsQueryHelpers.aggregationTerms("term_field", Map("option" -> "optionValue")) shouldBe Some(Map("terms" -> Map("field" -> "term_field", "option" -> "optionValue")))
  }

  "#aggregationRange" should "return map with 3 pairs when 'from' and 'to' are not empty" in {
    EsQueryHelpers.keyedAggregationRange("keyValue", Option(1), Option(2)) shouldBe Some(Map("key"-> "keyValue", "from" -> 1, "to" -> 2))
  }

  "#keyedAggregationRange" should "return map with 2 pairs when 'from' is not defined" in {
    EsQueryHelpers.keyedAggregationRange("keyValue", None, Option(2)) shouldBe Some(Map("key"-> "keyValue", "to" -> 2))
  }

  "#keyedAggregationRange" should "return map with 2 pairs when 'to' is not defined" in {
    EsQueryHelpers.keyedAggregationRange("keyValue", Option(1), None) shouldBe Some(Map("key"-> "keyValue", "from" -> 1))
  }

  "#keyedAggregationRange" should "return None when 'from' and 'to' are not defined" in {
    EsQueryHelpers.keyedAggregationRange("keyValue", None, None) shouldBe None
  }

  "#keyedAggregationRanges" should "should return a Map for range aggregations" in {
    val ranges = Seq(EsQueryHelpers.keyedAggregationRange("keyValue1", Option(1), Option(2)).get,
      EsQueryHelpers.keyedAggregationRange("keyValue2", Option(3), Option(4)).get,
      EsQueryHelpers.keyedAggregationRange("keyValue3", Option(5), Option(6)).get)

    EsQueryHelpers.keyedAggregationRanges("fieldName", ranges) shouldBe Some(Map("range" -> Map("field" -> "fieldName", "keyed" -> true,
      "ranges" -> Seq(Map("key" -> "keyValue1", "from" -> 1, "to" -> 2),
        Map("key" -> "keyValue2", "from" -> 3, "to" -> 4),
        Map("key" -> "keyValue3", "from" -> 5, "to" -> 6)))))
  }

  "geoDistance" should "should return a geo_distance Map" in {
    EsQueryHelpers.geoDistance("abc", "1", "2", "100km") shouldBe
      Some(Map("geo_distance" -> Map("distance" -> "100km", "abc" -> Map("lat" -> "1", "lon" -> "2"))))

  }

  behavior of "range"

  it should "have lower and upper bound" in {
    EsQueryHelpers.range("field", (gte, 5), Some(lt, 10)) shouldBe
      Some(
        Map("range" ->
          Map("field" ->
            Map("gte" -> 5.0,
              "lt" -> 10.0)
          )
        )
      )
  }

  it should "have only a lower bound" in {
    EsQueryHelpers.range("field", (gte, 5)) shouldBe
      Some(
        Map("range" ->
          Map("field" ->
            Map("gte" -> 5.0)
          )
        )
      )
  }

  it should "have inclusive lower and exclusive upper bound" in {
    EsQueryHelpers.range("field", (gt, 20), Some(lte, 30)) shouldBe
      Some(
        Map("range" ->
          Map("field" ->
          Map("gt" -> 20,
             "lte" -> 30)
          )
        )
      )
  }
}
