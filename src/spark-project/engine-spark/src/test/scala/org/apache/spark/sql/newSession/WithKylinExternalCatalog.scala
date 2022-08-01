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

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.NLocalFileMetadataTestCase

import java.io.File
import org.apache.kylin.engine.spark.mockup.external.FileCatalog
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.kylin.external.KylinSharedState
import org.apache.spark.sql.{SparderEnv, SparkSession}
import org.scalatest.BeforeAndAfterAll

trait WithKylinExternalCatalog extends SparkFunSuite with BeforeAndAfterAll {
  protected val ut_meta = "../examples/test_case_data/localmeta"
  protected val additional = "../../build/conf/spark-executor-log4j.xml"
  protected def externalCatalog: String = classOf[FileCatalog].getName
  protected def metadata : Seq[String] = {
      Seq(fitPathForUT(ut_meta))
  }
  private def fitPathForUT(path: String) = {
    if (new File(path).exists()) {
      path
    } else {
      s"../$path"
    }
  }

  protected def clearSparkSession(): Unit = {
    if (SparderEnv.isSparkAvailable) {
      SparderEnv.getSparkSession.stop()
    }
    SparkSession.setActiveSession(null)
    SparkSession.setDefaultSession(null)
  }

  protected lazy val spark: SparkSession = SparderEnv.getSparkSession
  lazy val kylinConf: KylinConfig = KylinConfig.getInstanceFromEnv
  lazy val metaStore: NLocalFileMetadataTestCase = new NLocalFileMetadataTestCase

  protected def overwriteSystemProp (key: String, value: String): Unit = {
    metaStore.overwriteSystemProp(key, value)
  }

  override def beforeAll(): Unit = {
    overwriteSystemProp("kylin.spark.discard-shard-state", "true")

    metaStore.createTestMetadata(metadata: _*)
    metaStore.overwriteSystemProp("kylin.use.external.catalog", externalCatalog)
    metaStore.overwriteSystemProp("kylin.NSparkDataSource.data.dir", s"${kylinConf.getMetadataUrlPrefix}/../data")
    metaStore.overwriteSystemProp("kylin.source.provider.9", "NSparkDataSource")
    metaStore.overwriteSystemProp("kylin.query.engine.sparder-additional-files", fitPathForUT(additional))
    metaStore.overwriteSystemProp("kylin.source.jdbc.adaptor", "Set By WithKylinExternalCatalog")
    metaStore.overwriteSystemProp("kylin.source.jdbc.driver", "Set By WithKylinExternalCatalog")
    metaStore.overwriteSystemProp("kylin.source.jdbc.connection-url", "Set By WithKylinExternalCatalog")


    assert(kylinConf.isDevOrUT)
    assert(spark.sharedState.isInstanceOf[KylinSharedState])
    assert(spark.sessionState.catalog.databaseExists("FILECATALOGUT"))
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    clearSparkSession()
    metaStore.restoreSystemProps()
    metaStore.cleanupTestMetadata()
  }
}
