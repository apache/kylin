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
package org.apache.spark.sql.kylin.external

import org.apache.kylin.externalCatalog.api.catalog.IExternalCatalog
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState, SharedState}
import org.apache.spark.util.Utils

import scala.util.control.NonFatal

class KylinSharedState (
    sc: SparkContext,
    private val externalCatalogClass: String) extends SharedState(sc, Map.empty) {
  override lazy val externalCatalog: ExternalCatalogWithListener = {
    val exteranl =
      KylinSharedState.instantiateExteranlCatalog(externalCatalogClass, sc.hadoopConfiguration)
    val externalCatalog = new KylinExternalCatalog(sc.conf, sc.hadoopConfiguration, exteranl)

    val defaultDbDefinition = CatalogDatabase(
      SessionCatalog.DEFAULT_DATABASE,
      "default database",
      CatalogUtils.stringToURI(conf.get(WAREHOUSE_PATH)),
      Map())

    // Create a default database in internal MemorySession Catalog even IExternalCatalog instance has one.
    // There may be another Spark application creating default database at the same time, here we
    // set `ignoreIfExists = true` to avoid `DatabaseAlreadyExists` exception.
    externalCatalog.createDatabase(defaultDbDefinition, ignoreIfExists = true)


    // Wrap to provide catalog events
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // Make sure we propagate external catalog events to the spark listener bus
    wrapped.addListener(new ExternalCatalogEventListener {
      override def onEvent(event: ExternalCatalogEvent): Unit = {
        sparkContext.listenerBus.post(event)
      }
    })
    wrapped
  }
}

class KylinSessionStateBuilder(
    session: SparkSession,
    state: Option[SessionState])
  extends BaseSessionStateBuilder(session, state) {

  override def build(): SessionState = {
    require(session.sharedState.isInstanceOf[KylinSharedState])
    super.build()
  }

  override protected lazy val catalog: KylinSessionCatalog = {
    val catalog = new KylinSessionCatalog(
      () => session.sharedState.externalCatalog,
      () => session.sharedState.globalTempViewManager,
      functionRegistry,
      tableFunctionRegistry,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader,
      session)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }
  override protected def newBuilder: NewBuilder = new KylinSessionStateBuilder(_, _)
}

object KylinSharedState extends Logging {
  def checkExternalClass(className: String): Boolean = {
    try {
      className match {
        case "" => false
        case _ =>
          Utils.classForName(className)
          true
      }
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError =>
        logWarning( s"Can't load Kylin external catalog $className, use Spark default instead")
        false
    }
  }
  private def instantiateExteranlCatalog(
      className: String,
      hadoopConfig: Configuration): IExternalCatalog = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(hadoopConfig).asInstanceOf[IExternalCatalog]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }
}
