package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}


class GetAliasByNameAction(builder: GetAliasByNameBuilder) extends GenericResultAbstractAction(builder) {
  val uriSuffix = builder.uriSuffix
  setURI(buildURI)

  def getRestMethodName: String = "GET"

  override def buildURI: String = s"_alias/$uriSuffix?format=json&bytes=b"

}


class GetAliasByNameBuilder(val aliasNames: Seq[String]) extends AbstractMultiTypeActionBuilder[GetAliasByNameAction, GetAliasByNameBuilder] {
  setHeader("content-type", "application/json")
  val uriSuffix = aliasNames.mkString(",")

  override def build: GetAliasByNameAction = new GetAliasByNameAction(this)
}
