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
package org.apache.spark.sql.delta

import java.util.Objects
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.datasource.storage.RewriteRuntimeConfig
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.PartitionDirectory

import scala.jdk.CollectionConverters._

import com.google.common.cache.{Cache, CacheBuilder}

import io.delta.standalone.actions.{AddFile => JAddFile}
import io.delta.standalone.expressions.{And => JAnd}
import io.delta.standalone.{DeltaLog => JDeltaLog}

case class KylinDeltaLogFileIndex(override val spark: SparkSession,
                                  override val deltaLog: DeltaLog,
                                  override val path: Path,
                                  snapshotAtAnalysis: Snapshot,
                                  partitionFilters: Seq[Expression] = Nil,
                                  isTimeTravelQuery: Boolean) extends TahoeFileIndex(spark, deltaLog, path)
  with Logging {

  private type DeltaExpressionCacheKey = (Seq[Expression], Seq[Expression])

  private type DeltaExpressionCacheValue = (Seq[AddFile], Long)

  val DeltaExpressionCache: Cache[(Seq[Expression], Seq[Expression]), (Seq[AddFile], Long)] = {
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(12, TimeUnit.HOURS)
    builder.build[DeltaExpressionCacheKey, DeltaExpressionCacheValue]()
  }

  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val exprCache = DeltaExpressionCache.get((partitionFilters, dataFilters), () => {
      val reloadFiles = listAddFiles(partitionFilters, dataFilters)
      val scanRecords = reloadFiles.map(_.numPhysicalRecords.get).sum
      DeltaExpressionCache.put((partitionFilters, dataFilters), (reloadFiles, scanRecords))
      (reloadFiles, scanRecords)
    })

    RewriteRuntimeConfig.adjustQueryRuntimeConfigs(spark, exprCache._1)

    makePartitionDirs(exprCache._1.groupBy(_.partitionValues).toSeq)
  }

  private def listAddFiles(partitionFilters: Seq[Expression],
                           dataFilters: Seq[Expression]): Seq[AddFile] = {
    matchingFiles(partitionFilters, dataFilters)
  }

  private def makePartitionDirs(partitionValuesToFiles:
                                Seq[(Map[String, String], Seq[AddFile])]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    partitionValuesToFiles.map {
      case (partitionValues, files) =>
        val rowValues: Array[Any] = partitionSchema.map { p =>
          val colName = DeltaColumnMapping.getPhysicalName(p)
          val partValue = Literal(partitionValues.get(colName).orNull)
          Cast(partValue, p.dataType, Option(timeZone), ansiEnabled = false).eval()
        }.toArray


        val fileStatuses = files.map { f =>
          new FileStatus(
            /* length */ f.size,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ f.modificationTime,
            absolutePath(f.path))
        }.toArray

        PartitionDirectory(new GenericInternalRow(rowValues), fileStatuses)
    }
  }

  def matchingFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[AddFile] = {
    val deltaLogCache = KylinDeltaCache.DeltaLogVersionCache.get((path, snapshotAtAnalysis.version), () => {
      reloadDeltaLogInfo(path)
    })

    if (partitionFilters.isEmpty && dataFilters.isEmpty) {
      return deltaLogCache._2.map(convertAddFileJ)
    }

    val jsnapshot = deltaLogCache._1.snapshot()
    val schema = jsnapshot.getMetadata.getSchema

    val finalFilter = new JAnd(
      SparkExpressionConverter.convertToStandaloneExpr(schema, partitionFilters),
      SparkExpressionConverter.convertToStandaloneExpr(schema, dataFilters)
    )

    val resultFiles = jsnapshot.scan(finalFilter).getFiles.asScala.map(convertAddFileJ).toSeq

    logInfo(s"Read from $path with file num ${resultFiles.size}. Before filter apply file num is ${deltaLogCache._2.size}.")

    resultFiles
  }

  private def reloadDeltaLogInfo(path: Path): (JDeltaLog, Seq[JAddFile]) = {
    logInfo(s"Reload delta log info from $path")
    val deltaLog = JDeltaLog.forTable(HadoopUtil.getCurrentConfiguration, path)
    val files = deltaLog.update().getAllFiles
    (deltaLog, files.asScala)
  }

  private def convertAddFileJ(external: JAddFile): AddFile = {
    AddFile(
      external.getPath,
      if (external.getPartitionValues == null) null else external.getPartitionValues.asScala.toMap,
      external.getSize,
      external.getModificationTime,
      external.isDataChange,
      external.getStats,
      if (external.getTags != null) external.getTags.asScala.toMap else null
    )
  }

  override def version: Long = {
    if (isTimeTravelQuery) snapshotAtAnalysis.version else deltaLog.unsafeVolatileSnapshot.version
  }

  // WARNING: These methods are intentionally pinned to the analysis-time snapshot, which may differ
  // from the one returned by [[getSnapshot]] that we will eventually scan.
  override def metadata: Metadata = snapshotAtAnalysis.metadata

  override def protocol: Protocol = snapshotAtAnalysis.protocol

  private def checkSchemaOnRead: Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SCHEMA_ON_READ_CHECK_ENABLED)
  }

  protected def getSnapshotToScan: Snapshot = {
    if (isTimeTravelQuery) snapshotAtAnalysis else deltaLog.update(stalenessAcceptable = true)
  }

  /** Provides the version that's being used as part of the scan if this is a time travel query. */
  def versionToUse: Option[Long] = if (isTimeTravelQuery) Some(snapshotAtAnalysis.version) else None

  def getSnapshot: Snapshot = {
    val snapshotToScan = getSnapshotToScan
    if (checkSchemaOnRead || snapshotToScan.metadata.columnMappingMode != NoMapping) {
      // Ensure that the schema hasn't changed in an incompatible manner since analysis time
      val snapshotSchema = snapshotToScan.metadata.schema
      if (!SchemaUtils.isReadCompatible(snapshotAtAnalysis.schema, snapshotSchema)) {
        throw DeltaErrors.schemaChangedSinceAnalysis(
          snapshotAtAnalysis.schema,
          snapshotSchema,
          mentionLegacyFlag = snapshotToScan.metadata.columnMappingMode == NoMapping)
      }
    }

    // disallow reading table with empty schema, which we support creating now
    if (snapshotToScan.schema.isEmpty) {
      // print the catalog identifier or delta.`/path/to/table`
      var message = TableIdentifier(deltaLog.dataPath.toString, Some("delta")).quotedString
      throw DeltaErrors.readTableWithoutSchemaException(message)
    }

    snapshotToScan
  }

  override def inputFiles: Array[String] = {
    getSnapshot
      .filesForScan(partitionFilters).files
      .map(f => absolutePath(f.path).toString)
      .toArray
  }

  override def refresh(): Unit = {}

  override def sizeInBytes: Long = deltaLog.unsafeVolatileSnapshot.sizeInBytes

  override def equals(that: Any): Boolean = that match {
    case t: KylinDeltaLogFileIndex =>
      t.path == path && t.deltaLog.isSameLogAs(deltaLog) &&
        t.versionToUse == versionToUse && t.partitionFilters == partitionFilters
    case _ => false
  }

  override def hashCode: scala.Int = {
    Objects.hashCode(path, deltaLog.compositeId, versionToUse, partitionFilters)
  }
}

