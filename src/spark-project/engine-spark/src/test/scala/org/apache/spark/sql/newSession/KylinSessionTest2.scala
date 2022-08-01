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
package org.apache.spark.sql.newSession

import org.apache.spark.sql.kylin.external.KylinSharedState
import org.apache.spark.sql.{KylinSession, SparderEnv, SparkSession}
import org.scalatest.BeforeAndAfterEach

/**
 * It is similar with [[org.apache.spark.sql.KylinSessionTest]], and placed here because it need load
 * [[org.apache.kylin.engine.spark.mockup.external.FileCatalog]]
 */
class KylinSessionTest2 extends WithKylinExternalCatalog with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
     clearSparkSession()
  }

  override def afterEach(): Unit = {
     clearSparkSession()
  }

  ignore("AL-91: For CustomCatalog") {

    val spark = SparderEnv.getSparkSession.asInstanceOf[KylinSession]
    assert(spark.sharedState.isInstanceOf[KylinSharedState])

    // DB
    // FileCatalog don't support create database
    assertResult(4)(spark.sql("show databases").count())

    // temp view
    import spark.implicits._
    val s = Seq(1, 2, 3).toDF("num")
    s.createOrReplaceTempView("nums")
    assert(spark.sessionState.catalog.getTempView("nums").isDefined)
    assert(SparkSession.getDefaultSession.isDefined)

    // UDF
    spark.sql("select ceil_datetime(date'2012-02-29', 'year')").collect()
      .map(row => row.toString()).mkString.equals("[2013-01-01 00:00:00.0]")
    spark.sparkContext.stop()

    // active
    assert(SparkSession.getActiveSession.isDefined)
    assert(SparkSession.getActiveSession.get eq spark)

    // default
    assert(SparkSession.getDefaultSession.isEmpty)

    val spark2 = SparderEnv.getSparkSession.asInstanceOf[KylinSession]
    assert(SparkSession.getActiveSession.isDefined)
    assert(SparkSession.getActiveSession.get eq spark2)
    assert(SparkSession.getDefaultSession.isDefined)
    assert(SparkSession.getDefaultSession.get eq spark2)

    // external catalog's reference should same
    assert(spark.sharedState.externalCatalog eq spark2.sharedState.externalCatalog)
    // DB
    assertResult(4)(spark2.sql("show databases").count())

    // temp view
    assert(spark2.sessionState.catalog.getTempView("nums").isDefined)

    // UDF
    spark2.sql("select ceil_datetime(date'2012-02-29', 'year')").collect()
      .map(row => row.toString()).mkString.equals("[2013-01-01 00:00:00.0]")
    spark2.stop()
  }
}
