/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common

import io.netty.util.internal.ThrowableUtil
import org.apache.kylin.query.engine.data.QueryResult
import org.apache.kylin.util.ExecAndComp.EnhancedQueryResult
import org.apache.kylin.util.{ExecAndComp, QueryResultComparator}
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
