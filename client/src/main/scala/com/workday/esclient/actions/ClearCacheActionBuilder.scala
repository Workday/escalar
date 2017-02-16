/*
 * Copyright 2016 Workday, Inc.
 *
 * This software is available under the MIT license.
 * Please see the LICENSE.txt file in this project.
 */

package com.workday.esclient.actions

import io.searchbox.indices.ClearCache

/** This class just widens visibility of the ClearCache constructor */
private[actions] class ClearCachePublic(builder: ClearCache.Builder) extends ClearCache(builder)

class ClearCacheActionBuilder extends ClearCache.Builder {
  override def build(): ClearCache = new ClearCachePublic(this)

  def filterKeys(keys: Seq[String]): ClearCache.Builder = setParameter("filter_keys", keys.mkString(","))
}
