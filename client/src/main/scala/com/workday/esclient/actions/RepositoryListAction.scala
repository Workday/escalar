package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

class RepositoryListBuilder extends AbstractMultiTypeActionBuilder[RepositoryListAction, RepositoryListBuilder] {

  override def build: RepositoryListAction = new RepositoryListAction(this)
}

class RepositoryListAction(builder: RepositoryListBuilder) extends GenericResultAbstractAction(builder) {
  setURI(buildURI)

  override def getRestMethodName: String = "GET"

  protected override def buildURI: String = s"_snapshot/_all"
}

case class ListRepositoryResponse(
  repositories: Seq[RepositoryDefinition]
)

case class RepositoryDefinition(
  repoName: String,
  repoType: String,
  settings: Map[String, Any]
)
