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
package org.apache.kylin.engine.spark.builder

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path}

import java.util
import org.apache.kylin.engine.spark.builder.CubeBuilderHelper.ENCODE_SUFFIX
import org.apache.kylin.engine.spark.job.NSparkCubingUtil._
import org.apache.kylin.engine.spark.metadata.{ColumnDesc, SegmentInfo}
import org.apache.spark.dict.NGlobalDictionary
import org.apache.spark.internal.Logging
import org.apache.spark.sql.KylinFunctions._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row, functions}
import org.apache.spark.utils.SparkVersionUtils

import scala.collection.JavaConverters._
import scala.collection.mutable._

object CubeTableEncoder extends Logging {

  def encodeTable(ds: Dataset[Row], seg: SegmentInfo, cols: util.Set[ColumnDesc], jobId: String): Dataset[Row] = {
    if (SparkVersionUtils.isLessThanSparkVersion("2.4", true)) {
      assert(!ds.sparkSession.conf.get("spark.sql.adaptive.enabled", "false").toBoolean,
        "Parameter 'spark.sql.adaptive.enabled' must be false when encode tables.")
    }
    val structType = ds.schema
    var partitionedDs = ds

    ds.sparkSession.sparkContext.setJobDescription("Encode count source data.")
    val sourceCnt = ds.count()
    val bucketThreshold = seg.kylinconf.getGlobalDictV2ThresholdBucketSize
    val minBucketSize: Long = sourceCnt / bucketThreshold

    var repartitionSizeAfterEncode = 0
    cols.asScala.foreach(
      ref => {
        val globalDict = new NGlobalDictionary(seg.project, ref.tableAliasName, ref.columnName, seg.kylinconf.getHdfsWorkingDirectory)
        val bucketSize = globalDict.getBucketSizeOrDefault(seg.kylinconf.getGlobalDictV2MinHashPartitions)
        val enlargedBucketSize = (((minBucketSize / bucketSize) + 1) * bucketSize).toInt
        if (enlargedBucketSize > repartitionSizeAfterEncode) {
          repartitionSizeAfterEncode = enlargedBucketSize;
        }

        val encodeColRef = convertFromDot(ref.identity)
        val columnIndex = structType.fieldIndex(encodeColRef)

        var dictParams = Array(seg.project, ref.tableAliasName, ref.columnName, seg.kylinconf.getHdfsWorkingDirectory)
          .mkString(SEPARATOR)
        val aliasName = structType.apply(columnIndex).name.concat(ENCODE_SUFFIX)
        var encodeCol = dict_encode(col(encodeColRef).cast(StringType), lit(dictParams), lit(bucketSize).cast(StringType)).as(aliasName)
        val columns = partitionedDs.schema.map(ty => col(ty.name))

        var scatterSkewedData = false
        if (seg.kylinconf.detectDataSkewInDictEncodingEnabled()) {
          //find skewed data in dict-encoding step
          val castEncodeColRef = col(encodeColRef).cast(StringType)
          val sampleData = ds.select(castEncodeColRef).sample(seg.kylinconf.sampleRateInEncodingSkewDetection()).cache()
          val totalCount = sampleData.count()
          val skewDictStorage = new Path(seg.kylinconf.getJobTmpDir(seg.project) +
            "/" + jobId + "/skewed_data/" + ref.identity)
          val skewedDict = new Object2LongOpenHashMap[String]()
          sampleData.groupBy(encodeColRef)
            .agg(functions.count(lit(1)).alias("count_value"))
            .filter(col("count_value") > totalCount * seg.kylinconf.skewPercentageThreshHold())
            .repartition(enlargedBucketSize, castEncodeColRef)
            .select(Seq(castEncodeColRef, encodeCol): _*)
            .collect().foreach(row => skewedDict.put(row.getString(0), row.getLong(1)));
          sampleData.unpersist()

          //save skewed data dict
          if (skewedDict.size() > 0) {
            scatterSkewedData = true
            val kryo = new Kryo()
            val fs = skewDictStorage.getFileSystem(new Configuration())
            if (fs.exists(skewDictStorage)) {
              fs.delete(skewDictStorage, true)
            }
            val output = new Output(fs.create(skewDictStorage))
            kryo.writeClassAndObject(output, skewedDict)
            output.close()

            //define repartition expression: repartition skewed data to random partition
            val scatterColumn = scatter_skew_data(castEncodeColRef, lit(skewDictStorage.toString))
              .alias("scatter_skew_data_" + ref.columnName)

            //encode cuboid table with skewed data dictionary
            dictParams = Array(seg.project, ref.tableAliasName, ref.columnName, seg.kylinconf.getHdfsWorkingDirectory, skewDictStorage.toString)
              .mkString(SEPARATOR)
            encodeCol = dict_encode(col(encodeColRef).cast(StringType), lit(dictParams), lit(bucketSize).cast(StringType)).alias(aliasName)

            partitionedDs = partitionedDs.select(columns ++ Seq(scatterColumn): _*)
              .repartition(enlargedBucketSize, col("scatter_skew_data_" + ref.columnName))
              .select(columns ++ Seq(encodeCol): _*)
          }
        }
        if (!scatterSkewedData) {
          partitionedDs = partitionedDs
            .repartition(enlargedBucketSize, col(encodeColRef).cast(StringType))
            .select(columns ++ Seq(encodeCol): _*)
        }
      }
    )

    ds.sparkSession.sparkContext.setJobDescription(null)

    //repartition by a single column during dict encode step before is more easily to cause data skew, add step to void such case.
    if (!cols.isEmpty && seg.kylinconf.rePartitionEncodedDatasetWithRowKey) {
      val colsInDS = partitionedDs.schema.map(_.name)
      val rowKeyColRefs = seg.allRowKeyCols.map(colDesc => convertFromDot(colDesc.identity)).filter(colsInDS.contains).map(col)
      //if not set in config, use the largest partition num during dict encode step
      if (seg.kylinconf.getRepartitionNumAfterEncode > 0) {
        repartitionSizeAfterEncode = seg.kylinconf.getRepartitionNumAfterEncode;
      }
      logInfo(s"repartition encoded dataset to $repartitionSizeAfterEncode partitions to avoid data skew")
      partitionedDs = partitionedDs.repartition(repartitionSizeAfterEncode, rowKeyColRefs.toArray: _*)
    }
    partitionedDs
  }
}