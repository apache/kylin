/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.job

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.exception.KylinException
import org.apache.kylin.common.exception.code.ErrorCodeJob.SANITY_CHECK_ERROR
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.metadata.cube.model.{IndexEntity, LayoutEntity}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.datasource.storage.StorageListener
import org.apache.spark.sql.functions.{col, sum}

import scala.collection.JavaConverters._

class SanityChecker(expect: Long) extends StorageListener with LogEx {

  override def onPersistBeforeRepartition(dataFrame: DataFrame, layout: LayoutEntity): Unit = {
    // just implement it
  }

  override def onPersistAfterRepartition(dataFrame: DataFrame, layout: LayoutEntity): Unit = {
    if (!KylinConfig.getInstanceFromEnv.isSanityCheckEnabled) {
      return
    }
    if (expect == SanityChecker.SKIP_FLAG) {
      log.info(s"Cannot find count constant measure in root, skip sanity check")
      return
    }

    val actual = SanityChecker.getCount(dataFrame, layout)

    if (actual == SanityChecker.SKIP_FLAG) {
      log.info(s"Cannot find count constant measure in current cuboid, skip sanity check")
      return
    }

    checkRowCount(actual, expect, layout)
  }

  private def checkRowCount(dfRowCount: Long, parentRowCount: Long, layout: LayoutEntity): Unit = {
    if (dfRowCount != parentRowCount) {
      throw new KylinException(SANITY_CHECK_ERROR, layout.getId + "")
    }
  }
}

object SanityChecker extends LogEx {

  val SKIP_FLAG: Long = -1L

  def getCount(df: DataFrame, layout: LayoutEntity): Long = {
    if (IndexEntity.isTableIndex(layout.getId)) {
      df.count()
    } else {
      layout.getOrderedMeasures.values().asScala.find(m => m.getFunction.isCountConstant && !m.isTomb) match {
        case Some(countMeasure) =>
          val countMeasureCol = countMeasure.getId.toString
          val row = df.select(countMeasureCol).agg(sum(col(countMeasureCol))).collect().head
          if (row.isNullAt(0)) 0L else row.getLong(0)
        case _ =>
          SKIP_FLAG
      }
    }
  }
}

