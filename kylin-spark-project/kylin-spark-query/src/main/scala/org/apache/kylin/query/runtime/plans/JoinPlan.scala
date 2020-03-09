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
