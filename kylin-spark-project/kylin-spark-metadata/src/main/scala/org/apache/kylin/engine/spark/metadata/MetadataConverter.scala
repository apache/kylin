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

package org.apache.kylin.engine.spark.metadata


import org.apache.kylin.cube.{CubeInstance, CubeUpdate}
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
import org.apache.spark.sql.util.SparkTypeUtil

import scala.collection.JavaConverters._
import org.apache.kylin.metadata.datatype.{DataType => KyDataType}
import org.apache.kylin.metadata.model.{DataModelDesc, JoinTableDesc, TableRef, TblColRef}

object MetadataConverter {
  def getSegmentInfo(cubeInstance: CubeInstance, segmentId: String): SegmentInfo = {
    extractEntity(cubeInstance)
    null
  }

  def getCubeUpdate(segmentInfo: SegmentInfo): CubeUpdate = {
    null
  }


  def extractJoinTable(cubeInstance: CubeInstance): Array[JoinDesc] = {
    cubeInstance.getModel.getAllTables
      .asScala
      .map(tb => toTableDesc(tb))

    cubeInstance.getModel.getJoinTables
      .map(join => toJoinDesc(join))
  }

  def toJoinDesc(joinTableDesc: JoinTableDesc): JoinDesc = {
    val desc = toTableDesc(joinTableDesc.getTableRef)
    val PKS = joinTableDesc.getJoin.getPrimaryKeyColumns.map(col => toColumnDesc(ref = col))
    val FKS = joinTableDesc.getJoin.getForeignKeyColumns.map(col => toColumnDesc(ref = col))
    JoinDesc(desc, PKS, FKS, joinTableDesc.getJoin.getType, joinTableDesc.getKind.equals(DataModelDesc.TableKind.LOOKUP))
  }

  def toTableDesc(tb: TableRef): TableDesc = {
    TableDesc(tb.getTableName, tb.getTableDesc.getDatabase, tb.getColumns.asScala.map(ref => toColumnDesc(ref = ref)).toList, tb.getAlias, 9)
  }

  def extractEntity(cubeInstance: CubeInstance): List[LayoutEntity] = {
    val dimensionMapping = cubeInstance.getDescriptor
      .getRowkey
      .getRowKeyColumns
      .map(co => (co.getColRef, co.getBitIndex))
      .toMap
    val dimensionMap = dimensionMapping
      .map { co =>
        (co._2, toColumnDesc(co._2, co._1))
      }
      .toMap
    val values = dimensionMap.keys.toList
    val measureId = cubeInstance
      .getMeasures
      .asScala
      .zipWithIndex
      .map { case (measure, in) =>
        val index = in + dimensionMap.size
        val parametrs = measure.getFunction.getParameter
          .getColRefs.asScala
          .map(col => toColumnDesc(dimensionMapping.apply(col), col))
          .toList
        val dataType = measure.getFunction.getReturnDataType
        (Integer.valueOf(index), FunctionDesc(measure.getName, DTType(dataType.getName, dataType.getPrecision),
          parametrs, measure.getFunction.getExpression))
      }.toMap.asJava

    cubeInstance.getDescriptor.getInitialCuboidScheduler
      .getAllCuboidIds
      .asScala
      .map { long =>
        val dimension = tailor(values, long)
        val orderDimension = dimension.map(index => (index, dimensionMap.apply(index))).toMap.asJava
        val entity = new LayoutEntity()
        entity.setOrderedDimensions(orderDimension)
        entity.setOrderedMeasures(measureId)
        entity
      }.toList
  }

  private def toColumnDesc(index: Int = -1, ref: TblColRef) = {
    val dataType = SparkTypeUtil.toSparkType(KyDataType.getType(ref.getDatatype))
    val columnDesc = if (ref.getColumnDesc.isComputedColumn) {
      ComputedColumnDesc(ref.getName, dataType, ref.getTableRef.getTableName, ref.getTableRef.getAlias,
        index, ref.getExpressionInSourceDB)
    } else {
      ColumnDesc(ref.getName, dataType, ref.getTableRef.getTableName, ref.getTableRef.getAlias, index)
    }
    columnDesc
  }

  private def tailor(complete: List[Int], cuboidId: Long): Array[Integer] = {
    val bitCount: Int = java.lang.Long.bitCount(cuboidId)
    val ret: Array[Integer] = new Array[Integer](bitCount)
    var next: Int = 0
    val size: Int = complete.size
    for (i <- 0 until size) {
      val shift: Int = size - i - 1
      if ((cuboidId & (1L << shift)) != 0)
        next += 1
      ret(next) = complete.apply(i)
    }
    ret
  }
}
