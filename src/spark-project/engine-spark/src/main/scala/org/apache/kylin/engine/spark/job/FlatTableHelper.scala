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

package org.apache.kylin.engine.spark.job

import org.apache.commons.lang3.StringUtils
import org.apache.kylin.engine.spark.builder.CreateFlatTable.replaceDot
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc
import org.apache.kylin.query.util.QueryUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

object FlatTableHelper extends Logging {

  def applyPartitionDesc(
                          flatTable: IJoinedFlatTableDesc,
                          ds: Dataset[Row],
                          needReplaceDot: Boolean): Dataset[Row] = {
    var afterFilter = ds
    val model = flatTable.getDataModel

    val partDesc = model.getPartitionDesc
    if (partDesc != null && partDesc.getPartitionDateColumn != null) {
      val segRange = flatTable.getSegRange
      if (segRange != null && !segRange.isInfinite) {
        var afterConvertPartition = partDesc.getPartitionConditionBuilder
          .buildDateRangeCondition(partDesc, null, segRange)
        if (needReplaceDot) afterConvertPartition = replaceDot(afterConvertPartition, model)
        logInfo(s"Partition filter $afterConvertPartition")
        afterFilter = afterFilter.where(afterConvertPartition)
      }
    }

    afterFilter
  }

  def applyFilterCondition(
                            flatTable: IJoinedFlatTableDesc,
                            ds: Dataset[Row],
                            needReplaceDot: Boolean): Dataset[Row] = {
    var afterFilter = ds
    val model = flatTable.getDataModel

    if (StringUtils.isNotBlank(model.getFilterCondition)) {
      var filterCond = model.getFilterCondition
      filterCond = QueryUtil.massageExpression(model, model.getProject, filterCond, null);
      if (needReplaceDot) filterCond = replaceDot(filterCond, model)
      filterCond = s" (1=1) AND (" + filterCond + s")"
      logInfo(s"Filter condition is $filterCond")
      afterFilter = afterFilter.where(filterCond)
    }

    afterFilter
  }
}
