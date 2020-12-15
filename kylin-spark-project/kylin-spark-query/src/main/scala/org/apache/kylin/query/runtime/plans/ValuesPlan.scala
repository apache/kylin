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
package org.apache.kylin.query.runtime.plans

import org.apache.kylin.query.relnode.OLAPValuesRel
import org.apache.spark.sql.{DataFrame, Row, SparkOperation}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.utils.SparkTypeUtil

import scala.collection.JavaConverters._

object ValuesPlan {
  def values(rel: OLAPValuesRel): DataFrame = {

    val schema = StructType(rel.getRowType.getFieldList.asScala.map { field =>
      StructField(
        field.getName,
        SparkTypeUtil.convertSqlTypeToSparkType(field.getType))
    })
    val rows = rel.tuples.asScala.map { tp =>
      Row.fromSeq(tp.asScala.map(lit => SparkTypeUtil.getValueFromRexLit(lit)))
    }.asJava
    if (rel.tuples.size() == 0) {
      SparkOperation.createEmptyDataFrame(schema)
    } else {
      SparkOperation.createConstantDataFrame(rows, schema)
    }
  }
}
