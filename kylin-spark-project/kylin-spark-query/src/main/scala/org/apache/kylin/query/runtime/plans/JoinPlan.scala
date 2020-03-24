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

import org.apache.kylin.query.relnode.OLAPJoinRel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import scala.collection.JavaConverters._

object JoinPlan {

  def join(
    inputs: java.util.List[DataFrame],
    rel: OLAPJoinRel): DataFrame = {

    val lDataFrame = inputs.get(0)
    val rDataFrame = inputs.get(1)
    val lSchemaNames = lDataFrame.schema.fieldNames.map("l_" + _)
    val rSchemaNames = rDataFrame.schema.fieldNames.map("r_" + _)
    // val schema = statefulDF.indexSchema
    val newLDataFrame = inputs.get(0).toDF(lSchemaNames: _*)
    val newRDataFrame = inputs.get(1).toDF(rSchemaNames: _*)
    var joinCol: Column = null

    //  todo   utils
    rel.getLeftKeys.asScala
      .zip(rel.getRightKeys.asScala)
      .foreach(tuple => {
        if (joinCol == null) {
          joinCol = col(lSchemaNames.apply(tuple._1))
            .equalTo(col(rSchemaNames.apply(tuple._2)))
        } else {
          joinCol = joinCol.and(
            col(lSchemaNames.apply(tuple._1))
              .equalTo(col(rSchemaNames.apply(tuple._2))))
        }
      })
    if (joinCol == null) {
      newLDataFrame.crossJoin(newRDataFrame)
    } else {
      newLDataFrame.join(newRDataFrame, joinCol, rel.getJoinType.lowerName)
    }
  }
}
