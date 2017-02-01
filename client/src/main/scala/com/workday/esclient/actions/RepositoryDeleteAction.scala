package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

class RepositoryDeleteBuilder(repositoryName: String)
  extends AbstractMultiTypeActionBuilder[RepositoryDeleteAction, RepositoryDeleteBuilder] {
  val repoName = repositoryName

  override def build: RepositoryDeleteAction = new RepositoryDeleteAction(this)
}

class RepositoryDeleteAction(builder: RepositoryDeleteBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.repoName
  setURI(buildURI)

  override def getRestMethodName: String = "DELETE"

  protected override def buildURI: String = s"_snapshot/$repository"
}
