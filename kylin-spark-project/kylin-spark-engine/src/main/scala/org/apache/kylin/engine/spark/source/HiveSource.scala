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

package org.apache.kylin.engine.spark.source

import java.util
import java.util.Locale

import org.apache.kylin.engine.spark.NSparkCubingEngine.NSparkCubingSource
import org.apache.kylin.engine.spark.metadata.TableDesc
import org.apache.kylin.metadata.model.IBuildable
import org.apache.kylin.source.{IReadableTable, ISampleDataDeployer, ISource, ISourceMetadataExplorer, SourcePartition}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.utils.SparkTypeUtil

class HiveSource extends ISource with Logging {
  /**
   * Return an adaptor that implements specified interface as requested by the build engine.
   * The IMRInput in particular, is required by the MR build engine.
   */
  override def adaptToBuildEngine[I](engineInterface: Class[I]): I = {
    new NSparkCubingSource() {
      override def getSourceData(table: TableDesc, ss: SparkSession, parameters: util.Map[String, String]): Dataset[Row] = {
        val colString = table.columns.map((tb) => tb.columnName).mkString(",")
        val sql = String.format(Locale.ROOT, "select %s from %s", colString, table.identity)
        val df = ss.sql(sql)
        val sparkSchema = df.schema
        logInfo("Source data sql is: " + sql)
        logInfo("Kylin schema " + table.toSchema.treeString)
        df.select(SparkTypeUtil.alignDataType(sparkSchema, table.toSchema): _*)
      }
    }.asInstanceOf[I]
  }

  override def enrichSourcePartitionBeforeBuild(buildable: IBuildable, srcPartition: SourcePartition): SourcePartition = ???

  override def getSourceMetadataExplorer: ISourceMetadataExplorer = ???

  override def createReadableTable(tableDesc: org.apache.kylin.metadata.model.TableDesc, uuid: String): IReadableTable = ???

  override def getSampleDataDeployer: ISampleDataDeployer = ???

  override def unloadTable(tableName: String, project: String): Unit = ???

  override def close(): Unit = ???
}
