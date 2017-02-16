/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.indices.ClearCache

/**
  * Utility class to widen visibility of the ClearCache constructor.
  * */
private[actions] class ClearCachePublic(builder: ClearCache.Builder) extends ClearCache(builder)

/**
  * Builder class for Clear Cache actions.
  */
class ClearCacheActionBuilder extends ClearCache.Builder {
  /**
    * Builds [[io.searchbox.indices.ClearCache]]
    * @return
    */
  override def build(): ClearCache = new ClearCachePublic(this)

  /**
    * Adds a sequence of filter keys to the Clear Cache builder.
    * @param keys Sequence of filters.
    * @return [[io.searchbox.indices.ClearCache.Builder]]
    */
  def filterKeys(keys: Seq[String]): ClearCache.Builder = setParameter("filter_keys", keys.mkString(","))
}
