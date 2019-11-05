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

package io.kyligence.kap.engine.spark.builder

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil._
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.util.{Failure, Success, Try}

object DFBuilderHelper extends Logging {

  val ENCODE_SUFFIX = "_KE_ENCODE"

  def filterCols(dsSeq: Seq[Dataset[Row]], needCheckCols: Set[TblColRef]): Set[TblColRef] = {
    needCheckCols -- dsSeq.flatMap(ds => filterCols(ds, needCheckCols))
  }

  def filterCols(ds: Dataset[Row], needCheckCols: Set[TblColRef]): Set[TblColRef] = {
    needCheckCols.filter(cc =>
      isValidExpr(convertFromDot(cc.getExpressionInSourceDB), ds))
  }

  def isValidExpr(colExpr: String, ds: Dataset[Row]): Boolean = {
    Try(ds.select(expr(colExpr))) match {
      case Success(_) =>
        true
      case Failure(_) =>
        false
    }
  }

  def chooseSuitableCols(ds: Dataset[Row], needCheckCols: Iterable[TblColRef]): Seq[Column] = {
    needCheckCols
      .filter(ref => isValidExpr(ref.getExpressionInSourceDB, ds))
      .map(ref => expr(convertFromDot(ref.getExpressionInSourceDB)).alias(convertFromDot(ref.getIdentity)))
      .toSeq
  }

  def time[R](msg: String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    logInfo(s"$msg. Elapsed time: ${t1 - t0} ms")
    result
  }

}
