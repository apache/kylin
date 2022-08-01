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
package org.apache.kylin.common

import java.io.File

import org.apache.kylin.metadata.cube.model.{NDataflow, NDataflowManager}
import org.apache.kylin.metadata.model.TableDesc
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.JavaConverters._

trait SparkSqlSource {


  def registerSSBTable(spark: SparkSession, project: String, dataflowId: String): Unit = {

    val config = KylinConfig.getInstanceFromEnv
    val dfMgr: NDataflowManager = NDataflowManager.getInstance(config, project)
    var df: NDataflow = dfMgr.getDataflow(StreamingTestConstant.DATAFLOW_ID)
    registSSBTable(config, spark, df.getModel.getRootFactTable.getTableDesc)
    val joinTables = df.getModel.getJoinTables
    joinTables.asScala.foreach { joinDesc =>
      registSSBTable(config, spark, joinDesc.getTableRef.getTableDesc)
    }

  }


  def registSSBTable(config: KylinConfig, ss: SparkSession, tableDesc: TableDesc): Unit = {

    val schema =
      StructType(
        tableDesc.getColumns.filter(!_.getName.equals("LO_MINUTES")).map { columnDescs =>
          StructField(columnDescs.getName, SparderTypeUtil.toSparkType(columnDescs.getType, false))
        }
      )
    val utMetaDir = System.getProperty(KylinConfig.KYLIN_CONF)
    val path = new File(utMetaDir, "data/" + tableDesc.getDatabase + "." + tableDesc.getName + ".csv").getAbsolutePath
    ss.read.option("delimiter", ",").schema(schema).csv(path).createOrReplaceGlobalTempView(tableDesc.getName)
  }


}
