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

import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.engine.spark.utils.StorageUtils.findCountDistinctMeasure
import org.apache.kylin.engine.spark.utils.{JobMetrics, Metrics, Repartitioner, StorageUtils}
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataSegment, NDataflow}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KapConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.LayoutEntityConverter._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasource.FilePruner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.Executors
import java.util.{Objects, List => JList}
import java.{lang, util}
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

case class WriteTaskStats(
                           numPartitions: Int,
                           numFiles: Long,
                           numBytes: Long,
                           numRows: Long,
                           sourceRows: Long,
                           numBucket: Int,
                           partitionValues: JList[String])

abstract class StorageStore extends Logging {

  protected val TEMP_FLAG = "_temp_"

  private[storage] var storageListener: Option[StorageListener] = None

  def setStorageListener(listener: StorageListener): Unit = storageListener = Some(listener)

  def save(
            layout: LayoutEntity,
            outputPath: Path,
            kapConfig: KapConfig,
            dataFrame: DataFrame): WriteTaskStats

  def read(
            dataflow: NDataflow, layout: LayoutEntity, sparkSession: SparkSession,
            extraOptions: Map[String, String] = Map.empty[String, String]): DataFrame

  def readSpecialSegment(
                          segment: NDataSegment, layout: LayoutEntity, sparkSession: SparkSession,
                          extraOptions: Map[String, String] = Map.empty[String, String]): DataFrame

  def readSpecialSegment(
                          segment: NDataSegment, layout: LayoutEntity, partitionId: java.lang.Long, sparkSession: SparkSession): DataFrame

  def collectFileCountAndSizeAfterSave(outputPath: Path, conf: Configuration): (Long, Long) = {
    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      val cs = HadoopUtil.getContentSummary(fs, outputPath)
      (cs.getFileCount, cs.getLength)
    } else {
      (0L, 0L)
    }
  }
}

class StorageStoreV1 extends StorageStore {
  override def save(layout: LayoutEntity, outputPath: Path, kapConfig: KapConfig, dataFrame: DataFrame): WriteTaskStats = {
    val (metrics: JobMetrics, rowCount: Long, hadoopConf: Configuration, bucketNum: Int) =
      repartitionWriter(layout, outputPath, kapConfig, dataFrame)
    val (fileCount, byteSize) = collectFileCountAndSizeAfterSave(outputPath, hadoopConf)
    checkAndWriterFastBitmapLayout(dataFrame, layout, kapConfig, outputPath)
    WriteTaskStats(0, fileCount, byteSize, rowCount, metrics.getMetrics(Metrics.SOURCE_ROWS_CNT), bucketNum, new util.ArrayList[String]())
  }

  private def repartitionWriter(layout: LayoutEntity, outputPath: Path, kapConfig: KapConfig, dataFrame: DataFrame) = {
    val ss = dataFrame.sparkSession
    val hadoopConf = ss.sparkContext.hadoopConfiguration
    val fs = outputPath.getFileSystem(hadoopConf)

    val tempPath = outputPath.toString + TEMP_FLAG + System.currentTimeMillis()
    val metrics = StorageUtils.writeWithMetrics(dataFrame, tempPath)
    val rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT)
    storageListener.foreach(_.onPersistBeforeRepartition(dataFrame, layout))

    val bucketNum = StorageUtils.calculateBucketNum(tempPath, layout, rowCount, kapConfig)
    val summary = HadoopUtil.getContentSummary(fs, new Path(tempPath))
    val repartitionThresholdSize = if (findCountDistinctMeasure(layout)) {
      kapConfig.getParquetStorageCountDistinctShardSizeRowCount
    } else {
      kapConfig.getParquetStorageShardSizeRowCount
    }
    val repartitioner = new Repartitioner(
      kapConfig.getParquetStorageShardSizeMB,
      kapConfig.getParquetStorageRepartitionThresholdSize,
      rowCount,
      repartitionThresholdSize,
      summary,
      layout.getShardByColumns,
      layout.getOrderedDimensions.keySet().asList(),
      kapConfig.optimizeShardEnabled()
    )
    repartitioner.doRepartition(outputPath.toString, tempPath, bucketNum, ss)
    storageListener.foreach(_.onPersistAfterRepartition(ss.read.parquet(outputPath.toString), layout))
    (metrics, rowCount, hadoopConf, bucketNum)
  }

  def checkAndWriterFastBitmapLayout(dataset: DataFrame, layoutEntity: LayoutEntity, kapConfig: KapConfig, layoutPath: Path): Unit = {
    if (!layoutEntity.getIndex.getIndexPlan.isFastBitmapEnabled) {
      return
    }
    val bitmaps = layoutEntity.listBitmapMeasure()
    if (bitmaps.isEmpty) {
      return
    }
    logInfo(s"Begin write fast bitmap cuboid. layout id is ${layoutEntity.getId}")
    val outputPath = new Path(layoutPath.toString + HadoopUtil.FAST_BITMAP_SUFFIX)

    def replaceCountDistinctEvalColumn(list: java.util.List[String], dataFrame: DataFrame): DataFrame = {
      val columns = dataFrame.schema.names.map(name =>
        if (list.contains(name)) {
          callUDF("eval_bitmap", col(name)).as(name)
        } else {
          col(name)
        })
      dataFrame.select(columns: _*)
    }

    val afterReplaced = replaceCountDistinctEvalColumn(bitmaps, dataset)
    repartitionWriter(layoutEntity, outputPath, kapConfig, afterReplaced)
  }


  override def read(dataflow: NDataflow, layout: LayoutEntity, sparkSession: SparkSession,
                    extraOptions: Map[String, String] = Map.empty[String, String]): DataFrame = {
    val structType = if ("true".equals(extraOptions.apply("isFastBitmapEnabled"))) {
      layout.toExactlySchema()
    } else {
      layout.toSchema()
    }
    val indexCatalog = new FilePruner(sparkSession, options = extraOptions, structType)
    sparkSession.baseRelationToDataFrame(
      HadoopFsRelation(
        indexCatalog,
        partitionSchema = indexCatalog.partitionSchema,
        dataSchema = indexCatalog.dataSchema.asNullable,
        bucketSpec = None,
        new ParquetFileFormat,
        options = extraOptions)(sparkSession))
  }

  override def readSpecialSegment(
                                   segment: NDataSegment, layout: LayoutEntity, sparkSession: SparkSession,
                                   extraOptions: Map[String, String]): DataFrame = {
    val layoutId = layout.getId
    val path = NSparkCubingUtil.getStoragePath(segment, layoutId)
    sparkSession.read.parquet(path)
  }

  override def readSpecialSegment(segment: NDataSegment, layout: LayoutEntity, //
                                  partitionId: lang.Long, sparkSession: SparkSession): DataFrame = {
    val layoutId = layout.getId
    val dataPartition = segment.getLayout(layoutId).getDataPartition(partitionId)
    require(Objects.nonNull(dataPartition))
    val path = NSparkCubingUtil.getStoragePath(segment, layoutId, dataPartition.getBucketId)
    sparkSession.read.parquet(path)
  }

}

object StorageStoreUtils extends Logging {
  def writeSkewData(bucketIds: Seq[Int], dataFrame: DataFrame, outputPath: Path, table: CatalogTable,
                    normalCase: Seq[Column], skewCase: Seq[Column], bucketNum: Int
                   ): Set[String] = {
    withNoSkewDetectScope(dataFrame.sparkSession) {
      val hadoopConf = dataFrame.sparkSession.sparkContext.hadoopConfiguration
      val fs = outputPath.getFileSystem(hadoopConf)
      val outputPathStr = outputPath.toString

      // filter out skew data, then write it separately
      val service = Executors.newCachedThreadPool()
      implicit val executorContext = ExecutionContext.fromExecutorService(service)
      val futures = {
        bucketIds.map { bucketId =>
          (dataFrame
            .filter(Column(HashPartitioning(normalCase.map(_.expr), bucketNum).partitionIdExpression) === bucketId)
            .repartition(bucketNum, skewCase: _*), new Path(outputPathStr + s"_temp_$bucketId"))
        } :+ {
          (dataFrame
            .filter(not(
              Column(HashPartitioning(normalCase.map(_.expr), bucketNum).partitionIdExpression).isin(bucketIds: _*)
            )).repartition(bucketNum, normalCase: _*), outputPath)
        }
      }.map { case (df, path) =>
        Future[(Path, Set[String])] {
          try {
            val partitionDirs =
              StorageStoreUtils.writeBucketAndPartitionFile(df, table, hadoopConf, path)
            (path, partitionDirs)
          } catch {
            case t: Throwable =>
              logError(s"Error for write skew data concurrently.", t)
              throw t
          }
        }
      }

      val results = try {
        val eventualFuture = Future.sequence(futures.toList)
        ThreadUtils.awaitResult(eventualFuture, Duration.Inf)
      } catch {
        case t: Throwable =>
          ThreadUtils.shutdown(service)
          throw t
      }

      // move skew data to final output path
      if (table.partitionColumnNames.isEmpty) {
        results.map(_._1).filter(!_.toString.equals(outputPathStr)).foreach { path =>
          fs.listStatus(path).foreach { file =>
            StorageUtils.overwriteWithMessage(fs, file.getPath, new Path(s"$outputPath/${file.getPath.getName}"))
          }
        }
      } else {
        logInfo(s"with partition column, results $results")
        results.filter(!_._1.toString.equals(outputPathStr))
          .foreach { case (path: Path, partitions: Set[String]) =>
            partitions.foreach { partition =>
              if (!fs.exists(new Path(s"$outputPath/$partition"))) {
                fs.mkdirs(new Path(s"$outputPath/$partition"))
              }
              fs.listStatus(new Path(s"$path/$partition")).foreach { file =>
                StorageUtils.overwriteWithMessage(fs, file.getPath, new Path(s"$outputPath/$partition/${file.getPath.getName}"))
              }
            }
          case _ => throw new RuntimeException
          }
      }
      results.flatMap(_._2).toSet
    }
  }

  def extractRepartitionColumns(table: CatalogTable, layout: LayoutEntity): (Seq[Column], Seq[Column]) = {
    (table.bucketSpec.isDefined, table.partitionColumnNames.nonEmpty) match {
      case (true, true) => (table.bucketSpec.get.bucketColumnNames.map(col), table.partitionColumnNames.map(col))
      case (false, true) => (table.partitionColumnNames.map(col), layout.getColOrder.asScala.map(id => col(id.toString)))
      case (true, false) => (table.bucketSpec.get.bucketColumnNames.map(col), layout.getColOrder.asScala.map(id => col(id.toString)))
      case (false, false) => (Seq.empty[Column], Seq.empty[Column])
    }
  }

  private def withNoSkewDetectScope[U](ss: SparkSession)(body: => U): U = {
    try {
      ss.sessionState.conf.setLocalProperty("spark.sql.adaptive.shuffle.maxTargetPostShuffleInputSize", "-1")
      body
    } catch {
      case e: Throwable => throw e
    }
    finally {
      ss.sessionState.conf.setLocalProperty("spark.sql.adaptive.shuffle.maxTargetPostShuffleInputSize", null)
    }
  }

  def toDF(segment: NDataSegment, layoutEntity: LayoutEntity, sparkSession: SparkSession): DataFrame = {
    StorageStoreFactory.create(layoutEntity.getModel.getStorageType).readSpecialSegment(segment, layoutEntity, sparkSession)
  }

  def toDF(segment: NDataSegment, layoutEntity: LayoutEntity, partitionId: java.lang.Long, sparkSession: SparkSession): DataFrame = {
    StorageStoreFactory.create(layoutEntity.getModel.getStorageType).readSpecialSegment(segment, layoutEntity, partitionId, sparkSession)
  }

  def writeBucketAndPartitionFile(
                                   dataFrame: DataFrame, table: CatalogTable, hadoopConf: Configuration,
                                   qualifiedOutputPath: Path): Set[String] = {
    dataFrame.sparkSession.sessionState.conf.setLocalProperty("spark.sql.adaptive.enabled.when.repartition", "true")
    var partitionDirs = Set.empty[String]
    runCommand(dataFrame.sparkSession, "UnsafelySave") {
      UnsafelyInsertIntoHadoopFsRelationCommand(qualifiedOutputPath, dataFrame.logicalPlan, table,
        set => partitionDirs = partitionDirs ++ set)
    }
    dataFrame.sparkSession.sessionState.conf.setLocalProperty("spark.sql.adaptive.enabled.when.repartition", null)
    partitionDirs
  }

  private def runCommand(session: SparkSession, name: String)(command: LogicalPlan): Unit = {
    val qe = session.sessionState.executePlan(command)
    /*try {
      val start = System.nanoTime()
      // call `QueryExecution.toRDD` to trigger the execution of commands.
      SQLExecution.withNewExecutionId(session, qe)(qe.toRdd)
      val end = System.nanoTime()
      session.listenerManager.onSuccess(name, qe, end - start)
    } catch {
      case e: Exception =>
        session.listenerManager.onFailure(name, qe, e)
        throw e
    }*/
  }
}

trait StorageListener {

  def onPersistBeforeRepartition(dataFrame: DataFrame, layout: LayoutEntity)

  def onPersistAfterRepartition(dataFrame: DataFrame, layout: LayoutEntity)

}



