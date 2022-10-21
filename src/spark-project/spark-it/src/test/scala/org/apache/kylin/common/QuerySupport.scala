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

import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.util.Unsafe
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.kylin.query.engine.QueryExec
import org.apache.kylin.query.util.{QueryParams, QueryUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.common.{SharedSparkSession, SparderQueryTest}
import org.apache.spark.sql.udf.UdfManager
import org.apache.spark.sql.{DataFrame, SparderEnv}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import java.util.Locale

trait QuerySupport
  extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging
    with SharedSparkSession {
  self: Suite =>
  val sparder = System.getProperty("kylin.query.engine.sparder-enabled")


  override def beforeAll(): Unit = {
    super.beforeAll()
    Unsafe.setProperty("kylin.query.engine.sparder-enabled", "true")
    UdfManager.create(spark)
    SparderEnv.setSparkSession(spark)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (sparder != null) {
      Unsafe.setProperty("kylin.query.engine.sparder-enabled", sparder)
    } else {
      Unsafe.clearProperty("kylin.query.engine.sparder-enabled")
    }
  }

  def singleQuery(sql: String, project: String): DataFrame = {
    val prevRunLocalConf = Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", "FALSE")
    try {
      val queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv)
      val queryParams = new QueryParams(NProjectManager.getProjectConfig(project), sql, project,
        0, 0, queryExec.getDefaultSchemaName, true)
      val convertedSql = QueryUtil.massageSql(queryParams)
      queryExec.executeQuery(convertedSql)
    } finally {
      if (prevRunLocalConf == null) {
        Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally")
      } else {
        Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", prevRunLocalConf)
      }
    }
    SparderEnv.getDF
  }

  def changeJoinType(sql: String, targetType: String): String = {
    if (targetType.equalsIgnoreCase("default")) return sql
    val specialStr = "changeJoinType_DELIMITERS"
    val replaceSql = sql.replaceAll(System.getProperty("line.separator"),
      " " + specialStr + " ")
    val tokens = StringUtils.split(replaceSql, null)
    // split white spaces
    var i = 0
    while (i < tokens.length - 1) {
      if ((tokens(i).equalsIgnoreCase("inner") || tokens(i).equalsIgnoreCase(
        "left")) &&
        tokens(i + 1).equalsIgnoreCase("join")) {
        tokens(i) = targetType.toLowerCase(Locale.ROOT)
      }
      i += 1
    }
    var ret = tokens.mkString(" ")
    ret = ret.replaceAll(specialStr, System.getProperty("line.separator"))
    ret
  }

  def checkWithSparkSql(sqlText: String, project: String): String = {
    val df = sql(sqlText)
    df.show(1000)
    SparderQueryTest.checkAnswer(df, singleQuery(sqlText, project))
  }
}
