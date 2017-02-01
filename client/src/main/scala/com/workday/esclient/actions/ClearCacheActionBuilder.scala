package com.workday.esclient.actions

import io.searchbox.indices.ClearCache

/** This class just widens visibility of the ClearCache constructor */
private[actions] class ClearCachePublic(builder: ClearCache.Builder) extends ClearCache(builder)

class ClearCacheActionBuilder extends ClearCache.Builder {
  override def build(): ClearCache = new ClearCachePublic(this)

  def filterKeys(keys: Seq[String]): ClearCache.Builder = setParameter("filter_keys", keys.mkString(","))
}
