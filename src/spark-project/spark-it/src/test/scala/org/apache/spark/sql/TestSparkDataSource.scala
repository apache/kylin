/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.sql

import io.kyligence.kap.common.SSSource
import org.apache.kylin.common.KylinConfig
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
    val kylinBuildEnv = KylinBuildEnv.getOrCreate(KylinConfig.getInstanceFromEnv);
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
