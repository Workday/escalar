package com.workday.esclient.actions

import io.searchbox.action.{AbstractMultiTypeActionBuilder, GenericResultAbstractAction}

class ClusterSettingsListBuilder extends AbstractMultiTypeActionBuilder[ClusterSettingsListAction, ClusterSettingsListBuilder] {

  override def build: ClusterSettingsListAction = new ClusterSettingsListAction(this)
}

class ClusterSettingsListAction(builder: ClusterSettingsListBuilder) extends GenericResultAbstractAction(builder) {
  setURI(buildURI)

  override def getRestMethodName: String = "GET"

  protected override def buildURI: String = s"_cluster/settings"
}
