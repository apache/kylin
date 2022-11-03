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

package io.kyligence.kap.common

import org.apache.kylin.util.ExecAndComp.EnhancedQueryResult
import org.apache.kylin.util.{ExecAndComp, QueryResultComparator}
import io.netty.util.internal.ThrowableUtil
import org.apache.kylin.query.engine.data.QueryResult
import org.scalatest.Suite

trait CompareSupport extends QuerySupport {
  self: Suite =>

  def runAndCompare(querySql: String,
                    project: String,
                    joinType: String,
                    filename: String,
                    checkOrder: Boolean,
                    sparkSql: Option[String] = None,
                    extraComparator: (EnhancedQueryResult, QueryResult) => Boolean = (_, _) => true): String = {
    try {
      val modelResult = ExecAndComp.queryModelWithOlapContext(project, joinType, querySql)

      var startTs = System.currentTimeMillis
      val sparkResult = ExecAndComp.queryWithSpark(project,
        ExecAndComp.removeDataBaseInSql(sparkSql.getOrElse(querySql)),
        joinType, filename)
      log.info("Query with Spark Duration(ms): {}", System.currentTimeMillis - startTs)

      startTs = System.currentTimeMillis
      var result = QueryResultComparator.compareResults(sparkResult, modelResult.getQueryResult,
        if (checkOrder) ExecAndComp.CompareLevel.SAME_ORDER else ExecAndComp.CompareLevel.SAME)
      result = result && extraComparator.apply(modelResult, sparkResult)
      log.info("Compare Duration(ms): {}", System.currentTimeMillis - startTs)

      if (!result) {
        val queryErrorMsg = s"$joinType\n$filename\n $querySql\n"
        if ("true".equals(System.getProperty("Failfast"))) {
          throw new RuntimeException(queryErrorMsg)
        }
        queryErrorMsg
      } else {
        null
      }
    } catch {
      case exception: Exception =>
        if ("true".equals(System.getProperty("Failfast"))) {
          throw exception
        } else {
          s"$joinType\n$filename\n $querySql\n" + ThrowableUtil.stackTraceToString(exception)
        }
    }
  }
}
