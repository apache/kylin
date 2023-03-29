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
package org.apache.kylin.engine.spark.builder.v3dict

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.builder.v3dict.DictBuildMode.{V2UPGRADE, V3APPEND, V3INIT, V3UPGRADE}
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.dict.{NBucketDictionary, NGlobalDictionaryV2}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkInternalAgent._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Window}
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import util.retry.blocking.RetryStrategy.RetryStrategyProducer
import util.retry.blocking.{Failure, Retry, RetryStrategy, Success}

import java.nio.file.Paths
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt

object DictionaryBuilder extends Logging {

  implicit val retryStrategy: RetryStrategyProducer =
    RetryStrategy.fixedBackOff(retryDuration = 10.seconds, maxAttempts = 5)

  def buildGlobalDict(
     project: String,
     spark: SparkSession,
     plan: LogicalPlan): LogicalPlan = transformCountDistinct(spark, plan) transform {

    case GlobalDictionaryPlaceHolder(expr: String, child: LogicalPlan, dbName: String) =>
      spark.sparkContext.setJobDescription(s"Build v3 dict $expr")
      val catalog = expr.split(NSparkCubingUtil.SEPARATOR)
      val tableName = catalog.apply(0)
      val columnName = catalog.apply(1)
      val context = new DictionaryContext(project, dbName, tableName, columnName, expr)

      // concurrent commit may cause delta ConcurrentAppendException.
      // so need retry commit incremental dict to delta table.
      Retry(incrementBuildDict(spark, child, context)) match {
        case Success(_) => logInfo(s"Incremental persist global dictionary for: $expr success.")
        case Failure(e) => logInfo(s"Incremental persist global dictionary for: $expr failure.", e)
      }
      spark.sparkContext.setJobDescription(null)

      val dictPath = getDictionaryPath(context)
      val dictPlan = getLogicalPlan(spark.read.format("delta").load(dictPath))
      val (key, value) = (dictPlan.output.head, dictPlan.output(1))
      val (existKey, existValue) = (child.output.head, child.output(1))
      val keyAlias = Alias(key, existKey.name)(existKey.exprId)
      val valueAlias = Alias(value, existValue.name)(existValue.exprId)
      Project(Seq(keyAlias, valueAlias), dictPlan)
  }

  /**
   * Generate an incremental dictionary encoding plan.
   * Use Left anti join to process raw data and dictionary tables.
   */
  private def transformerDictPlan(
     spark: SparkSession,
     context: DictionaryContext,
     plan: LogicalPlan): LogicalPlan = {

    val dictPath = getDictionaryPath(context)
    val dictTable: DeltaTable = DeltaTable.forPath(dictPath)
    val maxOffset = dictTable.toDF.count()
    plan match {
      case Project(_, Project(_, Window(_, _, _, windowChild))) =>
        val column = context.expr
        val windowSpec = org.apache.spark.sql.expressions.Window.orderBy(col(column))
        val joinCondition = createColumn(
          EqualTo(col(column).cast(StringType).expr,
            getLogicalPlan(dictTable.toDF).output.head))
        val filterKey = getLogicalPlan(dictTable.toDF).output.head.name
        val antiJoinDF = getDataFrame(spark, windowChild)
          .filter(col(filterKey).isNotNull)
          .join(dictTable.toDF,
            joinCondition,
            "left_anti")
          .select(col(column).cast(StringType) as "dict_key",
            (row_number().over(windowSpec) + lit(maxOffset)).cast(LongType) as "dict_value")
        getLogicalPlan(antiJoinDF)
      case _ => plan
    }
  }

  private def chooseDictBuildMode(context: DictionaryContext): DictBuildMode.Value = {
    val config = KylinConfig.getInstanceFromEnv
    if (isExistsV3Dict(context)) {
      V3APPEND
    } else if (isExistsOriginalV3Dict(context)) {
      V3UPGRADE
    } else if (config.isConvertV3DictEnable && isExistsV2Dict(context)) {
      V2UPGRADE
    } else V3INIT
  }

  /**
   * Build an incremental dictionary
   */
  private def incrementBuildDict(
    spark: SparkSession,
    plan: LogicalPlan,
    context: DictionaryContext): Unit = {
    val dictMode = chooseDictBuildMode(context)
    logInfo(s"V3 Dict build mode is $dictMode")
    dictMode match {
      case V3INIT =>
        val dictDF = getDataFrame(spark, plan)
        initAndSaveDictDF(dictDF, context)
      case V3APPEND =>
        mergeIncrementDict(spark, context, plan)
      // To be delete
      case V3UPGRADE =>
        val v3OrigDict = upgradeFromOriginalV3(spark, context)
        initAndSaveDictDF(v3OrigDict, context)
        mergeIncrementDict(spark, context, plan)
      case V2UPGRADE =>
        val v2Dict = upgradeFromV2(spark, context)
        initAndSaveDictDF(v2Dict, context)
        mergeIncrementDict(spark, context, plan)
    }
  }

  private def initAndSaveDictDF(dictDF: Dataset[Row], context: DictionaryContext): Unit = {
    val dictPath = getDictionaryPath(context)
    logInfo(s"Save dict values into path $dictPath.")
    dictDF.write.mode(SaveMode.Overwrite).format("delta").save(dictPath)
  }

  private def mergeIncrementDict(spark: SparkSession, context: DictionaryContext, plan: LogicalPlan): Unit = {
    val dictPlan = transformerDictPlan(spark, context, plan)
    val incrementDictDF = getDataFrame(spark, dictPlan)
    val dictPath = getDictionaryPath(context)
    logInfo(s"increment build global dict $dictPath")
    val dictTable = DeltaTable.forPath(dictPath)
    dictTable.alias("dict")
      .merge(incrementDictDF.alias("incre_dict"),
        "incre_dict.dict_key = dict.dict_key " +
          "and incre_dict.dict_value != dict.dict_value")
      .whenNotMatched().insertAll()
      .execute()
  }

  private def isExistsV2Dict(context: DictionaryContext): Boolean = {
    val config = KylinConfig.getInstanceFromEnv
    val globalDict = new NGlobalDictionaryV2(context.project,
      context.dbName + "." + context.tableName, context.columnName, config.getHdfsWorkingDirectory)
    val dictV2Meta = globalDict.getMetaInfo
    if (dictV2Meta != null) {
      logInfo(s"Exists V2 dict ${globalDict.getResourceDir}")
      true
    } else {
      logInfo(s"Not exists V2 dict ${globalDict.getResourceDir}")
      false
    }
  }

  private def isExistsV3Dict(context: DictionaryContext): Boolean = {
    val dictPath = getDictionaryPath(context)
    HadoopUtil.getWorkingFileSystem.exists(new Path(dictPath))
  }

  private def isExistsOriginalV3Dict(context: DictionaryContext): Boolean = {
    val dictPath = getOriginalDictionaryPath(context)
    HadoopUtil.getWorkingFileSystem.exists(new Path(dictPath))
  }

  private def fetchExistsOriginalV3Dict(context: DictionaryContext): Dataset[Row] = {
    val originalV3DictPath = getOriginalDictionaryPath(context)
    val v3dictTable = DeltaTable.forPath(originalV3DictPath)
    v3dictTable.toDF
  }

  private def transformCountDistinct(session: SparkSession, plan: LogicalPlan): LogicalPlan = {
    val transformer = new PreCountDistinctTransformer(session)
    transformer.apply(plan)
  }

  private def upgradeFromV2(spark: SparkSession, context: DictionaryContext): Dataset[Row] = {
    val config = KylinConfig.getInstanceFromEnv
    val globalDict = new NGlobalDictionaryV2(context.project,
      context.dbName + "." + context.tableName, context.columnName, config.getHdfsWorkingDirectory)
    val dictV2Meta = globalDict.getMetaInfo
    logInfo(s"Exists V2 dict ${globalDict.getResourceDir} num ${dictV2Meta.getDictCount}")
    val broadcastDict = spark.sparkContext.broadcast(globalDict)
    val dictSchema = new StructType(Array(StructField("dict_key", StringType),
      StructField("dict_value", LongType)))

    import spark.implicits._
    if (dictV2Meta != null) {
      spark.createDataset(0 to dictV2Meta.getBucketSize)
        .flatMap {
          bucketId =>
            val gDict: NGlobalDictionaryV2 = broadcastDict.value
            val bucketDict: NBucketDictionary = gDict.loadBucketDictionary(bucketId)
            val tupleList = new ListBuffer[Row]
            bucketDict.getAbsoluteDictMap
              .object2LongEntrySet
              .forEach(dictTuple => tupleList.append(Row.fromTuple(dictTuple.getKey, dictTuple.getLongValue)))
            tupleList.iterator
        }(RowEncoder.apply(dictSchema))
    } else {
      spark.emptyDataset[Row](RowEncoder.apply(dictSchema))
    }
  }

  private def upgradeFromOriginalV3(spark: SparkSession, context: DictionaryContext): Dataset[Row] = {
    if (isExistsOriginalV3Dict(context)) {
      fetchExistsOriginalV3Dict(context)
    } else {
      spark.emptyDataFrame
    }
  }

  private def getOriginalDictionaryPath(context: DictionaryContext): String = {
    val config = KylinConfig.getInstanceFromEnv
    val workingDir = config.getHdfsWorkingDirectory()
    val dictDir = new Path(context.project, new Path(HadoopUtil.GLOBAL_DICT_V3_STORAGE_ROOT,
      new Path(new Path(context.tableName), context.columnName)))
    workingDir + dictDir
  }

  def getDictionaryPath(context: DictionaryContext): String = {
    val config = KylinConfig.getInstanceFromEnv
    val workingDir = config.getHdfsWorkingDirectory()
    val dictDir = Paths.get(context.project,
      HadoopUtil.GLOBAL_DICT_V3_STORAGE_ROOT,
      context.dbName,
      context.tableName,
      context.columnName)
    workingDir + dictDir
  }

  def wrapCol(ref: TblColRef): String = {
    NSparkCubingUtil.convertFromDot(ref.getBackTickIdentity)
  }
}

class DictionaryContext(
   val project: String,
   val dbName: String,
   val tableName: String,
   val columnName: String,
   val expr: String)

object DictBuildMode extends Enumeration {

  val V3UPGRADE, V2UPGRADE, V3APPEND, V3INIT = Value

}