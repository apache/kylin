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

package org.apache.spark.sql.datasource.storage

import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.RandomUtil
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.FileFormatWriter.OutputSpec
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, FileFormatWriter}
import org.apache.spark.sql.{Row, SparkSession}

case class UnsafelyInsertIntoHadoopFsRelationCommand(
                                                      qualifiedOutputPath: Path,
                                                      query: LogicalPlan,
                                                      table: CatalogTable,
                                                      extractPartitionDirs: Set[String] => Unit
                                                    ) extends DataWritingCommand {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val outAttributes = query.output
    logInfo(s"Write data to $qualifiedOutputPath.")
    val partitionDirs = FileFormatWriter.write(
      sparkSession,
      child,
      DataSource.lookupDataSource("parquet", sparkSession.sessionState.conf).newInstance().asInstanceOf[FileFormat],
      FileCommitProtocol.instantiate(
        sparkSession.sessionState.conf.fileCommitProtocolClass,
        jobId = RandomUtil.randomUUID().toString,
        outputPath = qualifiedOutputPath.toString),
      OutputSpec(
        qualifiedOutputPath.toString, Map.empty, outAttributes),
      hadoopConf,
      outAttributes.filter(a => table.partitionColumnNames.contains(a.name)),
      table.bucketSpec,
      Seq(basicWriteJobStatsTracker(hadoopConf)),
      Map.empty
    )
    extractPartitionDirs(partitionDirs)
    Seq.empty
  }

  override def outputColumnNames: Seq[String] = query.output.map(_.name)

  override protected def withNewChildInternal(newChild: LogicalPlan): UnsafelyInsertIntoHadoopFsRelationCommand =
    copy(query = newChild)
}
