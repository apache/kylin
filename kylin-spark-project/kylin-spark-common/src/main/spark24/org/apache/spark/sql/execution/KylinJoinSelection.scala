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

package org.apache.spark.sql.execution

import javax.annotation.concurrent.GuardedBy
import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, RowOrdering}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SparkSession, Strategy}

/**
 * Select the proper physical plan for join based on joining keys and size of logical plan.
 *
 * At first, uses the [[ExtractEquiJoinKeys]] pattern to find joins where at least some of the
 * predicates can be evaluated by matching join keys. If found, join implementations are chosen
 * with the following precedence:
 *
 * - Broadcast hash join (BHJ):
 * BHJ is not supported for full outer join. For right outer join, we only can broadcast the
 * left side. For left outer, left semi, left anti and the internal join type ExistenceJoin,
 * we only can broadcast the right side. For inner like join, we can broadcast both sides.
 * Normally, BHJ can perform faster than the other join algorithms when the broadcast side is
 *     small. However, broadcasting tables is a network-intensive operation. It could cause OOM
 * or perform worse than the other join algorithms, especially when the build/broadcast side
 * is big.
 *
 * For the supported cases, users can specify the broadcast hint (e.g. the user applied the
 * [[org.apache.spark.sql.functions.broadcast()]] function to a DataFrame) and session-based
 * [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold to adjust whether BHJ is used and
 * which join side is broadcast.
 *
 * 1) Broadcast the join side with the broadcast hint, even if the size is larger than
 * [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]. If both sides have the hint (only when the type
 * is inner like join), the side with a smaller estimated physical size will be broadcast.
 * 2) Respect the [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold and broadcast the side
 * whose estimated physical size is smaller than the threshold. If both sides are below the
 * threshold, broadcast the smaller side. If neither is smaller, BHJ is not used.
 *
 * - Shuffle hash join: if the average size of a single partition is small enough to build a hash
 * table.
 *
 * - Sort merge: if the matching join keys are sortable.
 *
 * If there is no joining keys, Join implementations are chosen with the following precedence:
 * - BroadcastNestedLoopJoin (BNLJ):
 * BNLJ supports all the join types but the impl is OPTIMIZED for the following scenarios:
 * For right outer join, the left side is broadcast. For left outer, left semi, left anti
 * and the internal join type ExistenceJoin, the right side is broadcast. For inner like
 * joins, either side is broadcast.
 *
 * Like BHJ, users still can specify the broadcast hint and session-based
 * [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold to impact which side is broadcast.
 *
 * 1) Broadcast the join side with the broadcast hint, even if the size is larger than
 * [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]. If both sides have the hint (i.e., just for
 * inner-like join), the side with a smaller estimated physical size will be broadcast.
 * 2) Respect the [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold and broadcast the side
 * whose estimated physical size is smaller than the threshold. If both sides are below the
 * threshold, broadcast the smaller side. If neither is smaller, BNLJ is not used.
 *
 * - CartesianProduct: for inner like join, CartesianProduct is the fallback option.
 *
 * - BroadcastNestedLoopJoin (BNLJ):
 * For the other join types, BNLJ is the fallback option. Here, we just pick the broadcast
 * side with the broadcast hint. If neither side has a hint, we broadcast the side with
 * the smaller estimated physical size.
 */
case class KylinJoinSelection(session: SparkSession) extends Strategy with PredicateHelper with Logging {

  val conf: SQLConf = session.sessionState.conf

  /**
   * Matches a plan whose output should be small enough to be used in broadcast join.
   */
  private def canBroadcast(plan: LogicalPlan): Boolean = {
    val sizeInBytes = plan.stats.sizeInBytes
    sizeInBytes >= 0 && sizeInBytes <= conf.autoBroadcastJoinThreshold && JoinMemoryManager.acquireMemory(sizeInBytes.toLong)
  }

  /**
   * Matches a plan whose single partition should be small enough to build a hash table.
   *
   * Note: this assume that the number of partition is fixed, requires additional work if it's
   * dynamic.
   */
  private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
    plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
  }

  /**
   * Returns whether plan a is much smaller (3X) than plan b.
   *
   * The cost to build hash map is higher than sorting, we should only build hash map on a table
   * that is much smaller than other one. Since we does not have the statistic for number of rows,
   * use the size of bytes here as estimation.
   */
  private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
    a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
  }

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }

  private def broadcastSide(
                             canBuildLeft: Boolean,
                             canBuildRight: Boolean,
                             left: LogicalPlan,
                             right: LogicalPlan): BuildSide = {

    def smallerSide =
      if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft

    if (canBuildRight && canBuildLeft) {
      // Broadcast smaller side base on its estimated physical size
      // if both sides have broadcast hint
      smallerSide
    } else if (canBuildRight) {
      BuildRight
    } else if (canBuildLeft) {
      BuildLeft
    } else {
      // for the last default broadcast nested loop join
      smallerSide
    }
  }

  private def canBroadcastByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
  : Boolean = {
    val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
    val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
    buildLeft || buildRight
  }

  private def broadcastSideByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
  : BuildSide = {
    val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
    val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
    broadcastSide(buildLeft, buildRight, left, right)
  }

  private def canBroadcastBySizes(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
  : Boolean = {
    val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
    val buildRight = canBuildRight(joinType) && canBroadcast(right)
    buildLeft || buildRight
  }

  private def broadcastSideBySizes(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
  : BuildSide = {
    val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
    val buildRight = canBuildRight(joinType) && canBroadcast(right)
    broadcastSide(buildLeft, buildRight, left, right)
  }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // --- BroadcastHashJoin --------------------------------------------------------------------

    // broadcast hints were specified
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if canBroadcastByHints(joinType, left, right) =>
      val buildSide = broadcastSideByHints(joinType, left, right)
      Seq(joins.BroadcastHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, planLater(left), planLater(right)))

    // broadcast hints were not specified, so need to infer it from size and configuration.
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if canBroadcastBySizes(joinType, left, right) =>
      val buildSide = broadcastSideBySizes(joinType, left, right)
      Seq(joins.BroadcastHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, planLater(left), planLater(right)))

    // --- ShuffledHashJoin ---------------------------------------------------------------------

    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if !conf.preferSortMergeJoin && canBuildRight(joinType) && canBuildLocalHashMap(right)
        && muchSmaller(right, left) ||
        !RowOrdering.isOrderable(leftKeys) =>
      Seq(joins.ShuffledHashJoinExec(
        leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if !conf.preferSortMergeJoin && canBuildLeft(joinType) && canBuildLocalHashMap(left)
        && muchSmaller(left, right) ||
        !RowOrdering.isOrderable(leftKeys) =>
      Seq(joins.ShuffledHashJoinExec(
        leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

    // --- SortMergeJoin ------------------------------------------------------------

    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if RowOrdering.isOrderable(leftKeys) =>
      joins.SortMergeJoinExec(
        leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

    // --- Without joining keys ------------------------------------------------------------

    // Pick BroadcastNestedLoopJoin if one side could be broadcast
    case j@logical.Join(left, right, joinType, condition)
      if canBroadcastByHints(joinType, left, right) =>
      val buildSide = broadcastSideByHints(joinType, left, right)
      joins.BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

    case j@logical.Join(left, right, joinType, condition)
      if canBroadcastBySizes(joinType, left, right) =>
      val buildSide = broadcastSideBySizes(joinType, left, right)
      joins.BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

    // Pick CartesianProduct for InnerJoin
    case logical.Join(left, right, _: InnerLike, condition) =>
      joins.CartesianProductExec(planLater(left), planLater(right), condition) :: Nil

    case logical.Join(left, right, joinType, condition) =>
      val buildSide = broadcastSide(
        left.stats.hints.broadcast, right.stats.hints.broadcast, left, right)
      // This join could be very slow or OOM
      joins.BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

    // --- Cases where this strategy does not apply ---------------------------------------------

    case _ => Nil
  }
}

object JoinMemoryManager extends Logging {

  @GuardedBy("this")
  private[this] var memoryUsed: Long = 0

  def acquireMemory(numBytesToAcquire: Long): Boolean = synchronized {
    assert(numBytesToAcquire >= 0)
    val enoughMemory = numBytesToAcquire <= (maxMemoryJoinCanUse - memoryUsed)
    if (enoughMemory) {
      memoryUsed += numBytesToAcquire
      logInfo(s"Acquire $numBytesToAcquire bytes for BHJ, memory used $memoryUsed, max memory BHJ can use $maxMemoryJoinCanUse.")
    } else {
      logInfo("Driver memory is not enough for BHJ.")
    }
    enoughMemory
  }

  private def maxMemoryJoinCanUse: Long = {
    val joinMemoryFraction = KylinConfig.getInstanceFromEnv.getJoinMemoryFraction
    (Runtime.getRuntime.maxMemory() * joinMemoryFraction).toLong
  }

  def releaseAllMemory(): Unit = synchronized {
    memoryUsed = 0
  }

}