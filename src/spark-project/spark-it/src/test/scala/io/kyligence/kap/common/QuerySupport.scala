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

import io.kyligence.kap.query.util.KapQueryUtil

import java.util.Locale
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.Unsafe
import org.apache.kylin.query.engine.QueryExec
import org.apache.kylin.query.util.QueryParams
import org.apache.spark.internal.Logging
import org.apache.spark.sql.common.{SharedSparkSession, SparderQueryTest}
import org.apache.spark.sql.udf.UdfManager
import org.apache.spark.sql.{DataFrame, SparderEnv}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

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
      val queryParams = new QueryParams(KapQueryUtil.getKylinConfig(project), sql, project,
        0, 0, queryExec.getDefaultSchemaName, true)
      val convertedSql = KapQueryUtil.massageSql(queryParams)
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
