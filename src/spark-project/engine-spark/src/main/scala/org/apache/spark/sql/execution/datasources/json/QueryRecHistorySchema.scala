/*
* Copyright (C) 2016 Kyligence Inc. All rights reserved.
*
* http://kyligence.io
*
* This software is the confidential and proprietary information of
* Kyligence Inc. ("Confidential Information"). You shall not disclose
* such Confidential Information and shall use it only in accordance
* with the terms of the license agreement you entered into with
* Kyligence Inc.
*
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
*/

package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.sql.types._

object QueryRecHistorySchema {
  def queryRecHistorySchema(): StructType = {
    val recDetailInfoMeta = Array(
      StructField("modelId", StringType, nullable = false),
      StructField("semanticVersion", IntegerType, nullable = false),
      StructField("layouts", ArrayType(layoutRecInfoSchema())),
      StructField("layoutRecs", ArrayType(layoutRecInfoSchema()))
    )

    StructType(Array(
      StructField("queryTime", LongType),
      StructField("cpuTime", LongType),
      StructField("duration", LongType),
      StructField("recDetailMap", MapType(StringType, DataTypes.createStructType(recDetailInfoMeta)))
    ))
  }

  def layoutRecInfoSchema(): StructType = {
    val recMeasureMeta = Array(
      StructField("name", StringType),
      StructField("function", DataTypes.createStructType(
        Array(
          StructField("expression", StringType),
          StructField("parameters", ArrayType(DataTypes.createStructType(
            Array(
              StructField("type", StringType),
              StructField("value", StringType)
            ),
          )
          )
          ))
      )
      ),
      StructField("column", StringType)
    )
    DataTypes.createStructType(Array(
      StructField("uniqueId", StringType, nullable = false),
      StructField("columns", ArrayType(StringType)),
      StructField("dimensions", ArrayType(StringType)),
      StructField("shardBy", ArrayType(StringType)),
      StructField("sortBy", ArrayType(StringType)),
      StructField("measures", ArrayType(DataTypes.createStructType(recMeasureMeta))),
      StructField("ccExpression", DataTypes.createMapType(StringType, StringType)),
      StructField("ccType", DataTypes.createMapType(StringType, StringType))
    ))
  }

  def flatRecInfoSchema(): StructType = {
    StructType(Array(
      StructField("recId", StringType, nullable = false),
      StructField("day", StringType, nullable = false),
      StructField("cpuTime", LongType, nullable = false),
      StructField("detailInfo", layoutRecInfoSchema(), nullable = false)
    ))
  }

  def flatRecInfoSchema2(): StructType = {
    StructType(Array(
      StructField("recId", StringType, nullable = false),
      StructField("cpuTime", LongType, nullable = false),
      StructField("recInfo", layoutRecInfoSchema(), nullable = false)
    ))
  }

  def dailyStatsSchema(): StructType = {
    StructType(Array(
      StructField("recId", StringType, nullable = false),
      StructField("countNum", LongType, nullable = false),
      StructField("sumCpuTime", LongType, nullable = false),
      StructField("day", StringType, nullable = false),
      StructField("detailInfo", layoutRecInfoSchema(), nullable = false)
    ))
  }

}
