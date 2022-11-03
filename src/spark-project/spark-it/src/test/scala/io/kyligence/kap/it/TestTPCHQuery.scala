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

package io.kyligence.kap.it

import io.kyligence.kap.common.{JobSupport, QuerySupport}
import io.kyligence.kap.query.{QueryConstants, QueryFetcher}
import org.apache.kylin.common.util.Unsafe
import org.apache.kylin.query.relnode.OLAPContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite, SparderQueryTest}

import scala.collection.JavaConverters._

class TestTPCHQuery
  extends SparderBaseFunSuite
    with LocalMetadata
    with JobSupport
    with QuerySupport
    with SharedSparkSession
    with Logging {
  override val schedulerInterval = "10"
  override val DEFAULT_PROJECT = "tpch-pqt"

  override def beforeEach(): Unit = {
    SparderEnv.skipCompute()
  }

  override def afterEach(): Unit = {
    SparderEnv.cleanCompute()
  }

  ignore("tpch query") {
    //    System.setProperty("spark.local", "true")
    Unsafe.setProperty("kylin.query.engine.sparder-enabled", "true")
    // 22
    val sqlTime = QueryFetcher
      .fetchQueries(QueryConstants.KYLIN_SQL_BASE_DIR + "sql_tpch")
      .filter(_._1.contains("16"))
      .map { tp =>
        val start = System.currentTimeMillis()
        sql(tp._2).count()
        System.currentTimeMillis() - start
      }
    val kylinTim = QueryFetcher
      .fetchQueries(QueryConstants.KYLIN_SQL_BASE_DIR + "sql_tpch")
      .filter(_._1.contains("16"))
      .map { tp =>
        val df = singleQuery(tp._2, DEFAULT_PROJECT)
        val start = System.currentTimeMillis()
        Range.apply(0, 1).foreach(t => df.count())
        System.currentTimeMillis() - start
        OLAPContext.getThreadLocalContexts.asScala
          .map(_.realization.getUuid)
          .zip(OLAPContext.getThreadLocalContexts.asScala.map(
            _.storageContext.getLayoutId))
          .mkString(",")
      }
    val kylinEnd = System.currentTimeMillis()

    val tuples = sqlTime.zip(kylinTim)
    tuples.foreach(println)
    Thread.sleep(1000000000)
  }

  ignore("tpch build and query") {
    Unsafe.setProperty("spark.local", "true")
    Unsafe.setProperty("kylin.query.engine.sparder-enabled", "true")
    buildAll()
    QueryFetcher
      .fetchQueries(QueryConstants.KYLIN_SQL_BASE_DIR + "sql_tpch")
      //      .filter(_._1.contains("22"))
      .map { tp =>
        print(tp._1)
        print(tp._2)
        //        val df = sql(cleanSql(tp._2))
        val df = sql(tp._2)
        df.show(10)
        val kylinDf = singleQuery(tp._2, DEFAULT_PROJECT)
        var str = SparderQueryTest.checkAnswer(df, kylinDf)
        if (str != null) {
          str = tp._1 + "\n" + tp._2 + "\n" + str
          logInfo(tp._1 + "\n" + tp._2 + "\n" + str)
        }
        str
      }
      .filter(_ != null) foreach (logInfo(_))
  }
}
