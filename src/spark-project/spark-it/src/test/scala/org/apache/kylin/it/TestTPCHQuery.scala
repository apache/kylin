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

package org.apache.kylin.it

import org.apache.kylin.common.util.Unsafe
import org.apache.kylin.common.{JobSupport, QuerySupport}
import org.apache.kylin.query.relnode.OLAPContext
import org.apache.kylin.query.{QueryConstants, QueryFetcher}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite, SparderQueryTest}

import scala.collection.JavaConverters._

import io.kyligence.kap.common.{JobSupport, QuerySupport}
import io.kyligence.kap.query.{QueryConstants, QueryFetcher}

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
        Range.apply(0, 1).foreach(_ => df.count())
        System.currentTimeMillis() - start
        OLAPContext.getThreadLocalContexts.asScala
          .map(_.realization.getUuid)
          .zip(OLAPContext.getThreadLocalContexts.asScala.map(
            _.storageContext.getLayoutId))
          .mkString(",")
      }

    val tuples = sqlTime.zip(kylinTim)
    Thread.sleep(1000000000)
  }

  ignore("tpch build and query") {
    Unsafe.setProperty("spark.local", "true")
    Unsafe.setProperty("kylin.query.engine.sparder-enabled", "true")
    buildAll()
    QueryFetcher
      .fetchQueries(QueryConstants.KYLIN_SQL_BASE_DIR + "sql_tpch")
      .map { tp =>
        print(tp._1)
        print(tp._2)
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
