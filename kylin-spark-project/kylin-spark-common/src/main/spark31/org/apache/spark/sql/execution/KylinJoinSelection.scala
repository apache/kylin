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

import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, RowOrdering}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SparkSession, Strategy}

import javax.annotation.concurrent.GuardedBy

/**
 * .
 */
case class KylinJoinSelection(session: SparkSession) extends Strategy
  with JoinSelectionHelper
  with PredicateHelper
  with Logging {

  val conf: SQLConf = session.sessionState.conf

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // If it is an equi-join, we first look at the join hints w.r.t. the following order:
    //   1. broadcast hint: pick broadcast hash join if the join type is supported. If both sides
    //      have the broadcast hints, choose the smaller side (based on stats) to broadcast.
    //   2. sort merge hint: pick sort merge join if join keys are sortable.
    //   3. shuffle hash hint: We pick shuffle hash join if the join type is supported. If both
    //      sides have the shuffle hash hints, choose the smaller side (based on stats) as the
    //      build side.
    //   4. shuffle replicate NL hint: pick cartesian product if join type is inner like.
    //
    // If there is no hint or the hints are not applicable, we follow these rules one by one:
    //   1. Pick broadcast hash join if one side is small enough to broadcast, and the join type
    //      is supported. If both sides are small, choose the smaller side (based on stats)
    //      to broadcast.
    //   2. Pick shuffle hash join if one side is small enough to build local hash map, and is
    //      much smaller than the other side, and `spark.sql.join.preferSortMergeJoin` is false.
    //   3. Pick sort merge join if the join keys are sortable.
    //   4. Pick cartesian product if join type is inner like.
    //   5. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
    //      other choice.
    case j@ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, nonEquiCond, left, right, hint) =>
      def createBroadcastHashJoin(onlyLookingAtHint: Boolean) = {
        getBroadcastBuildSide(left, right, joinType, hint, onlyLookingAtHint, conf).map {
          buildSide =>
            Seq(joins.BroadcastHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              nonEquiCond,
              planLater(left),
              planLater(right)))
        }
      }

      def createShuffleHashJoin(onlyLookingAtHint: Boolean) = {
        getShuffleHashJoinBuildSide(left, right, joinType, hint, onlyLookingAtHint, conf).map {
          buildSide =>
            Seq(joins.ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              nonEquiCond,
              planLater(left),
              planLater(right)))
        }
      }

      def createSortMergeJoin() = {
        if (RowOrdering.isOrderable(leftKeys)) {
          Some(Seq(joins.SortMergeJoinExec(
            leftKeys, rightKeys, joinType, nonEquiCond, planLater(left), planLater(right))))
        } else {
          None
        }
      }

      def createCartesianProduct() = {
        if (joinType.isInstanceOf[InnerLike]) {
          // `CartesianProductExec` can't implicitly evaluate equal join condition, here we should
          // pass the original condition which includes both equal and non-equal conditions.
          Some(Seq(joins.CartesianProductExec(planLater(left), planLater(right), j.condition)))
        } else {
          None
        }
      }

      def createJoinWithoutHint() = {
        createBroadcastHashJoin(false)
          .orElse {
            if (!conf.preferSortMergeJoin) {
              createShuffleHashJoin(false)
            } else {
              None
            }
          }
          .orElse(createSortMergeJoin())
          .orElse(createCartesianProduct())
          .getOrElse {
            // This join could be very slow or OOM
            val buildSide = getSmallerSide(left, right)
            Seq(joins.BroadcastNestedLoopJoinExec(
              planLater(left), planLater(right), buildSide, joinType, nonEquiCond))
          }
      }

      createBroadcastHashJoin(true)
        .orElse {
          if (hintToSortMergeJoin(hint)) createSortMergeJoin() else None
        }
        .orElse(createShuffleHashJoin(true))
        .orElse {
          if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None
        }
        .getOrElse(createJoinWithoutHint())

    case j@ExtractSingleColumnNullAwareAntiJoin(leftKeys, rightKeys) =>
      Seq(joins.BroadcastHashJoinExec(leftKeys, rightKeys, LeftAnti, BuildRight,
        None, planLater(j.left), planLater(j.right), isNullAwareAntiJoin = true))

    // If it is not an equi-join, we first look at the join hints w.r.t. the following order:
    //   1. broadcast hint: pick broadcast nested loop join. If both sides have the broadcast
    //      hints, choose the smaller side (based on stats) to broadcast for inner and full joins,
    //      choose the left side for right join, and choose right side for left join.
    //   2. shuffle replicate NL hint: pick cartesian product if join type is inner like.
    //
    // If there is no hint or the hints are not applicable, we follow these rules one by one:
    //   1. Pick broadcast nested loop join if one side is small enough to broadcast. If only left
    //      side is broadcast-able and it's left join, or only right side is broadcast-able and
    //      it's right join, we skip this rule. If both sides are small, broadcasts the smaller
    //      side for inner and full joins, broadcasts the left side for right join, and broadcasts
    //      right side for left join.
    //   2. Pick cartesian product if join type is inner like.
    //   3. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
    //      other choice. It broadcasts the smaller side for inner and full joins, broadcasts the
    //      left side for right join, and broadcasts right side for left join.
    case logical.Join(left, right, joinType, condition, hint) =>
      val desiredBuildSide = if (joinType.isInstanceOf[InnerLike] || joinType == FullOuter) {
        getSmallerSide(left, right)
      } else {
        // For perf reasons, `BroadcastNestedLoopJoinExec` prefers to broadcast left side if
        // it's a right join, and broadcast right side if it's a left join.
        // TODO: revisit it. If left side is much smaller than the right side, it may be better
        // to broadcast the left side even if it's a left join.
        if (canBuildBroadcastLeft(joinType)) BuildLeft else BuildRight
      }

      def createBroadcastNLJoin(buildLeft: Boolean, buildRight: Boolean) = {
        val maybeBuildSide = if (buildLeft && buildRight) {
          Some(desiredBuildSide)
        } else if (buildLeft) {
          Some(BuildLeft)
        } else if (buildRight) {
          Some(BuildRight)
        } else {
          None
        }

        maybeBuildSide.map { buildSide =>
          Seq(joins.BroadcastNestedLoopJoinExec(
            planLater(left), planLater(right), buildSide, joinType, condition))
        }
      }

      def createCartesianProduct() = {
        if (joinType.isInstanceOf[InnerLike]) {
          Some(Seq(joins.CartesianProductExec(planLater(left), planLater(right), condition)))
        } else {
          None
        }
      }

      def createJoinWithoutHint() = {
        createBroadcastNLJoin(canBroadcastBySize(left, conf), canBroadcastBySize(right, conf))
          .orElse(createCartesianProduct())
          .getOrElse {
            // This join could be very slow or OOM
            Seq(joins.BroadcastNestedLoopJoinExec(
              planLater(left), planLater(right), desiredBuildSide, joinType, condition))
          }
      }

      createBroadcastNLJoin(hintToBroadcastLeft(hint), hintToBroadcastRight(hint))
        .orElse {
          if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None
        }
        .getOrElse(createJoinWithoutHint())

    // --- Cases where this strategy does not apply ---------------------------------------------
    case _ => Nil
  }

  override def canBroadcastBySize(plan: LogicalPlan, conf: SQLConf): Boolean = {
    val size = plan.stats.sizeInBytes
    size >= 0 && size <= conf.autoBroadcastJoinThreshold && JoinMemoryManager.acquireMemory(size.toLong)
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