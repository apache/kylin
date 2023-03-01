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

package org.apache.kylin.engine.spark.job.stage.build

import com.google.common.collect.{Maps, Queues}
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.job.stage.build.GenerateFlatTable._
import org.apache.kylin.engine.spark.job.{SanityChecker, SegmentJob}
import org.apache.kylin.job.execution.ExecutableState
import org.apache.kylin.metadata.cube.model._
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._

class GenerateFlatTable(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends FlatTableAndDictBase(jobContext, dataSegment, buildParam) {

  private var dataCountCheckGood: Boolean = true

  override def execute(): Unit = {
    buildParam.setFlatTable(generateFlatTable())
    buildParam.setFlatTablePart(generateFlatTablePart())
    buildParam.setBuildFlatTable(this)

    if (!checkDataCountPass()) {
      drainEmptyLayoutOnDataCountCheckFailed()
      return
    }
  }

  override def getStageName: String = "GenerateFlatTable"

  override def onStageFinished(state: ExecutableState): Unit = {
    if (dataCountCheckGood) {
      super.onStageFinished(state)
    } else {
      updateStageInfo(ExecutableState.WARNING.toString, null,
        Maps.newHashMap(Map(NBatchConstants.P_WARNING_CODE -> NDataLayout.AbnormalType.DATA_INCONSISTENT.name()).asJava))
    }
  }

  private def checkDataCountPass(): Boolean = {
    if (jobContext.getConfig.isDataCountCheckEnabled) {
      logInfo(s"segment ${dataSegment.getId} dataCountCheck is enabled")
      val result = checkDataCount()
      logInfo(s"segment ${dataSegment.getId} dataCountCheck result: ${result}")
      return result
    }
    logInfo(s"segment ${dataSegment.getId} dataCountCheck is not enabled")
    true
  }

  private def drainEmptyLayoutOnDataCountCheckFailed(): Unit = {
    dataCountCheckGood = false
    jobContext.setSkipFollowingStages(dataSegment.getId)
    readOnlyLayouts.asScala.foreach(newEmptyDataLayout(_, NDataLayout.AbnormalType.DATA_INCONSISTENT))
    drain()
  }

  /**
   * Data count check for segment layouts before index building, including 2 checks:
   * <p>Check1: Equality check for sum value of count among existing layouts
   * (not the layouts that are under construction)
   * <p>Check2: Equality check for sum value of count between layouts above and flat table
   *
   * <p>Special case for check1 is to allow un-equality count sum between agg layouts and table layouts
   * when <tt>kylin.build.allow-non-strict-count-check</tt> enabled, and should not be used in most cases.
   *
   * <p>Check2 will not execute when all the new layouts can be covered by existing layouts in index building.
   *
   * <p>Count measure will be used in calculating sum value of count for agg layouts,
   * and for table layouts simply counting data row numbers.
   *
   * @return <tt>true</tt> if data count check pass, <tt>false</tt> otherwise
   */
  private def checkDataCount(): Boolean = {
    val layouts = dataSegment.getSegDetails.getEffectiveLayouts.asScala.map(lay => jobContext.getIndexPlan.getLayoutEntity(lay.getLayoutId))
    val tasks = layouts.map(layout => new DataCountCheckTask(layout, StorageStoreUtils.toDF(dataSegment, layout, sparkSession)))
    val resultsQueue = Queues.newLinkedBlockingQueue[DataCountCheckResult]()

    if (layouts.isEmpty) return true

    // Start calculating data count
    slowStartExec(tasks.iterator, (task: DataCountCheckTask) => {
      val layout = task.layout
      val ds = task.ds
      resultsQueue.offer(new DataCountCheckResult(layout, SanityChecker.getCount(ds, layout)))
    })

    val reduceFor: (DataCountCheckResult => Boolean) => Long = layoutCountReducer(resultsQueue.asScala)
    val aggLayoutCount = reduceFor(result => !IndexEntity.isTableIndex(result.layout.getId))
    val tableLayoutCount = reduceFor(result => IndexEntity.isTableIndex(result.layout.getId))

    // All agg layouts count or table layouts count must be same
    if (isInvalidCount(aggLayoutCount) || isInvalidCount(tableLayoutCount)) {
      logWarning(s"segment ${dataSegment.getId} dataCountCheck check1 failed, " +
        s"count number in agg layouts or table layouts are not same, " +
        s"agg layouts count: ${aggLayoutCount}, table layouts count: ${tableLayoutCount}")
      return false
    }
    // Count number between agg layout and table layout should be same when non-strict count check is not enabled
    if (bothLayoutsExist(aggLayoutCount, tableLayoutCount)
      && (aggLayoutCount != tableLayoutCount && !config.isNonStrictCountCheckAllowed)) {
      logWarning(s"segment ${dataSegment.getId} dataCountCheck check1 failed, " +
        s"count number between agg layouts and table layouts are not same, " +
        s"agg layouts count: ${aggLayoutCount}, table layouts count: ${tableLayoutCount}")
      return false
    }
    // Agg layout count equals table layout count, comparing with flat table count or simply return true
    val layoutCount = if (isLayoutExists(aggLayoutCount)) aggLayoutCount else tableLayoutCount
    if (isLayoutExists(layoutCount) && buildParam.getSpanningTree.fromFlatTable) {
      val flatTableCount = buildParam.getFlatTable.count
      val check2Result = layoutCount == flatTableCount
      if (!check2Result) {
        logWarning(s"segment ${dataSegment.getId} dataCountCheck check2 failed, " +
        s"layouts count: ${layoutCount}, flat table count: ${flatTableCount}")
      }
      check2Result
    } else {
      true
    }
  }

  private def layoutCountReducer(results: Iterable[DataCountCheckResult])(filter: DataCountCheckResult => Boolean): Long = {
    results.filter(filter)
      .map(_.count)
      .reduceOption((prev, cur) => if (prev == cur) cur else InvalidCountFlag)
      .getOrElse(LayoutNonExistsFlag)
  }

  sealed class DataCountCheckTask(val layout: LayoutEntity, val ds: Dataset[Row]) extends Task {
    override def getTaskDesc: String = s"layout ${layout.getId} data count check"
  }

  sealed class DataCountCheckResult(val layout: LayoutEntity, val count: Long)
}

object GenerateFlatTable {
  val InvalidCountFlag: Long = SanityChecker.SKIP_FLAG
  val LayoutNonExistsFlag: Long = -2L

  def isInvalidCount(count: Long): Boolean = {
    count == InvalidCountFlag
  }

  def isLayoutExists(count: Long): Boolean = {
    count > LayoutNonExistsFlag
  }

  def bothLayoutsExist(aggLayoutCount: Long, tableLayoutCount: Long): Boolean = {
    aggLayoutCount > LayoutNonExistsFlag && tableLayoutCount > LayoutNonExistsFlag
  }
}
