package com.workday.esclient.actions

import io.searchbox.cluster.Health

object IndexHealthAction {
  val DEFAULT_TIMEOUT = "50ms"
}

class IndexHealthAction(builder: IndexHealthBuilder) extends Health(builder) {
  protected override def buildURI: String = super.buildURI + builder.indices.mkString(",") + "?timeout=" + builder.timeout
}

class IndexHealthBuilder(val indices: Seq[String] = Nil, val timeout: String = IndexHealthAction.DEFAULT_TIMEOUT) extends Health.Builder {
  override def build: IndexHealthAction = new IndexHealthAction(this)
}
