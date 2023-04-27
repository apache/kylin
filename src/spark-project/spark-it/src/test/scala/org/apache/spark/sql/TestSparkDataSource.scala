/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.kylin.common.{KylinConfig, SSSource}
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.source.NSparkCubingSourceInput
import org.apache.kylin.metadata.model.{ColumnDesc, TableDesc}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.apache.spark.sql.types.DataTypes
import org.junit.Assert

class TestSparkDataSource extends SparderBaseFunSuite
  with LocalMetadata
  with SSSource
  with Logging {

  test("test spark data source") {
    KylinBuildEnv.getOrCreate(KylinConfig.getInstanceFromEnv)
    val tableDesc = new TableDesc
    tableDesc.setName("test_country")
    val columnDescs = spark.table("test_country").schema
      .fieldNames
      .zipWithIndex
      .map { tp =>
        val columnDesc = new ColumnDesc(tp._2.toString, tp._1, "varchar(255)", "", null, null, null)
        columnDesc.setDatatype("varchar(255)")
        columnDesc
      }
    tableDesc.setColumns(columnDescs.drop(1))
    val input = new NSparkCubingSourceInput
    val df = input.getSourceData(tableDesc, spark, null)
    df.collect()
    // only 3 column
    Assert.assertEquals(3, df.schema.size)
    // all data type cast to string
    Assert.assertTrue(df.schema.forall(fd => fd.dataType.sameType(DataTypes.StringType)))
  }
}
