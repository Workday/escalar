package com.workday.esclient.actions

import com.google.gson.Gson
import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

class RepositoryCreateBuilder(repositoryName: String, repositoryDefinition: String)
  extends AbstractMultiTypeActionBuilder[RepositoryCreateAction, RepositoryCreateBuilder] {
  val repository = repositoryName
  val definition = repositoryDefinition

  override def build: RepositoryCreateAction = new RepositoryCreateAction(this)
}

class RepositoryCreateAction(builder: RepositoryCreateBuilder) extends GenericResultAbstractAction(builder) {
  val repository = builder.repository
  val definition = builder.definition
  setURI(buildURI)

  override def getRestMethodName: String = "PUT"

  override def getData(gson: Gson): String = definition

  protected override def buildURI: String = s"_snapshot/$repository"
}
