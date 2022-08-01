/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Intersect, Join, JoinHint, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ConvertInnerJoinToSemiJoinSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Convert inner join to semi", FixedPoint(1),
        new ConvertInnerJoinToSemiJoin()
      ) :: Nil
  }

  test("replace Inner Join with Left-semi Join") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int, 'e.int)
    val agg = Aggregate(Seq('c, 'e), Seq('c, 'e), table2)
    val join = Join(table1, agg, Inner, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)
    val project = Project(Seq('a, 'b), join)

    val optimized = Optimize.execute(project.analyze)

    val correctAnswer =
      Project(Seq('a, 'b),
        Join(table1,
          Project(Seq('c, 'e), table2),
          LeftSemi, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Inner Join with Left-semi Join - agg on the left side") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int, 'e.int)
    val agg = Aggregate(Seq('c, 'e), Seq('c, 'e), table2)
    val join = Join(agg, table1, Inner, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)
    val project = Project(Seq('a, 'b), join)

    val optimized = Optimize.execute(project.analyze)

    val correctAnswer =
      Project(Seq('a, 'b),
        Join(table1,
          Project(Seq('c, 'e), table2),
          LeftSemi, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("should not replace") {
    {
      val table1 = LocalRelation('a.int, 'b.int)
      val table2 = LocalRelation('c.int, 'd.int)
      val agg = Aggregate(Seq('c, 'd), Seq('c, 'd), table2)
      val join = Join(table1, agg, Inner, Option('a <=> 'c), JoinHint.NONE)
      val project = Project(Seq('a, 'b), join)

      val optimized = Optimize.execute(project.analyze)
      comparePlans(optimized, project.analyze)
    }

    {
      val table1 = LocalRelation('a.int, 'b.int)
      val table2 = LocalRelation('c.int, 'd.int)
      val agg = Aggregate(Seq('c, 'd), Seq('c), table2)
      val join = Join(table1, agg, Inner, Option('a <=> 'c), JoinHint.NONE)
      val project = Project(Seq('a, 'b), join)

      val optimized = Optimize.execute(project.analyze)
      comparePlans(optimized, project.analyze)
    }
  }


}
