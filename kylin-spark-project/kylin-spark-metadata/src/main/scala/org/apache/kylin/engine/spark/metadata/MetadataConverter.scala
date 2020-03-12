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


import java.lang
import java.util.regex.Pattern

import org.apache.commons.lang.StringUtils
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.cube.{CubeInstance, CubeSegment, CubeUpdate}
import org.apache.kylin.cube.cuboid.Cuboid
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
import org.apache.kylin.engine.spark.metadata.cube.BitUtils
import org.apache.spark.sql.utils.SparkTypeUtil

import scala.collection.JavaConverters._
import org.apache.kylin.metadata.datatype.{DataType => KyDataType}
import org.apache.kylin.metadata.model.{JoinTableDesc, TableRef, TblColRef}

import scala.collection.mutable

object MetadataConverter {
  def getSegmentInfo(cubeInstance: CubeInstance, segmentId: String): SegmentInfo = {
    val allColumnDesc = extractAllColumnDesc(cubeInstance)
    val (layoutEntities, measure) = extractEntityAndMeasures(cubeInstance)
    val dictColumn = measure.asScala.values.filter(_.returnType.dataType.equals("bitmap"))
      .map(_.pra.head).toSet
    SegmentInfo(segmentId, cubeInstance.getProject, cubeInstance.getConfig, extractFactTable(cubeInstance),
      extractLookupTable(cubeInstance), extractLookupTable(cubeInstance),
      extractJoinTable(cubeInstance), allColumnDesc.values.toList, layoutEntities, mutable.Set[LayoutEntity](layoutEntities: _*),
      dictColumn,
      dictColumn,
      extractPartitionExp(cubeInstance.getSegmentById(segmentId)),
      extractFilterCondition(cubeInstance.getSegmentById(segmentId)))
  }

  def getCubeUpdate(segmentInfo: SegmentInfo): CubeUpdate = {
    null
  }

  def extractFactTable(cubeInstance: CubeInstance): TableDesc = {
    toTableDesc(cubeInstance.getModel.getRootFactTable)
  }

  def extractLookupTable(cubeInstance: CubeInstance): List[TableDesc] = {
    cubeInstance.getModel.getJoinTables
      .filter(_.getKind.equals(org.apache.kylin.metadata.model.DataModelDesc.TableKind.LOOKUP))
      .map(join => toTableDesc(join.getTableRef))
      .toList
  }

  def extractJoinTable(cubeInstance: CubeInstance): Array[JoinDesc] = {
    cubeInstance.getModel.getAllTables
      .asScala
      .map(tb => toTableDesc(tb))
    val table = cubeInstance.getModel.getJoinsTree.getTableChains
      .asScala.keys.toArray // must to be order
      .filter(!_.equals(cubeInstance.getModel.getRootFactTable.getAlias))
    val tableMap = cubeInstance.getModel.getJoinTables
      .map(join => (join.getAlias, toJoinDesc(join)))
      .toMap
    table.map(tableMap.apply)
  }

  def toJoinDesc(joinTableDesc: JoinTableDesc): JoinDesc = {
    val desc = toTableDesc(joinTableDesc.getTableRef)
    val PKS = joinTableDesc.getJoin.getPrimaryKeyColumns.map(col => toColumnDesc(ref = col))
    val FKS = joinTableDesc.getJoin.getForeignKeyColumns.map(col => toColumnDesc(ref = col))
    JoinDesc(desc, PKS, FKS, joinTableDesc.getJoin.getType)
  }

  def toTableDesc(tb: TableRef): TableDesc = {
    TableDesc(tb.getTableName, tb.getTableDesc.getDatabase,
      tb.getColumns.asScala.map(ref => toColumnDesc(ref = ref)).toList, tb.getAlias, tb.getTableDesc.getSourceType)
  }

  def extractAllColumnDesc(cubeInstance: CubeInstance): Map[Int, ColumnDesc] = {
    val dimensionMapping = cubeInstance.getDescriptor
      .getRowkey
      .getRowKeyColumns
      .map(co => (co.getColRef, co.getBitIndex))
      .toMap
    val refs = cubeInstance.getAllColumns.asScala
      .filter(col => !dimensionMapping.contains(col))
      .zipWithIndex
      .map(tp => (tp._1, tp._2 + dimensionMapping.size))
    val columnIndex = dimensionMapping ++ refs
    columnIndex
      .map { co =>
        (co._2, toColumnDesc(co._1, co._2))
      }
  }

  def toLayoutEntity(cubeInstance: CubeInstance, cuboid: Cuboid): LayoutEntity = {
    val (columnIndexes, idToColumnMap, measureId) = genIDToColumnMap(cubeInstance)
    genLayoutEntity(columnIndexes, idToColumnMap, measureId, cuboid.getId)
  }

  def extractEntityAndMeasures(cubeInstance: CubeInstance): (List[LayoutEntity], java.util.Map[Integer, FunctionDesc]) = {
    val (columnIndexes, idToColumnMap, measureId) = genIDToColumnMap(cubeInstance)
    (cubeInstance.getDescriptor.getInitialCuboidScheduler
      .getAllCuboidIds
      .asScala
      .map { long =>
        genLayoutEntity(columnIndexes, idToColumnMap, measureId, long)
      }.toList, measureId)
  }

  private def genLayoutEntity(
    columnIndexes: List[Integer], idToColumnMap: Map[Int, ColumnDesc],
    measureId: java.util.Map[Integer, FunctionDesc], long: lang.Long) = {
    val dimension = BitUtils.tailor(columnIndexes.asJava, long)
    val orderDimension = dimension.asScala.map(index => (index, idToColumnMap.apply(index))).toMap.asJava
    val entity = new LayoutEntity()
    entity.setId(long)
    entity.setOrderedDimensions(orderDimension)
    entity.setOrderedMeasures(measureId)
    entity
  }

  private def genIDToColumnMap(cubeInstance: CubeInstance): (List[Integer], Map[Int, ColumnDesc], java.util.Map[Integer, FunctionDesc]) = {
    val dimensionMapping = cubeInstance.getDescriptor
      .getRowkey
      .getRowKeyColumns
      .map(co => (co.getColRef, co.getBitIndex))
      .toMap
    val refs = cubeInstance.getAllColumns.asScala
      .filter(col => !dimensionMapping.contains(col))
      .zipWithIndex
      .map(tp => (tp._1, tp._2 + dimensionMapping.size))
    val columnIDs = dimensionMapping ++ refs
    val IDToColumn = columnIDs
      .map { co =>
        (co._2, toColumnDesc(co._1, co._2))
      }.toMap
    val idToColumnMap = dimensionMapping.values.map(Integer.valueOf).toList
    val measureId = cubeInstance
      .getMeasures
      .asScala
      .zipWithIndex
      .map { case (measure, in) =>
        val index = in + IDToColumn.size
        val parameter = measure.getFunction.getParameter
        val dataType = measure.getFunction.getReturnDataType
        val parametrs = parameter.getType match {
          case "column" =>
            parameter.getColRefs.asScala
              .map(col => IDToColumn.apply(columnIDs.apply(col)))
              .toList
          case "constant" =>
            List(LiteralColumnDesc(null, SparkTypeUtil.toSparkType(dataType), null, null, -1, parameter.getValue))
        }
        (Integer.valueOf(index),
          FunctionDesc(measure.getName, DTType(dataType.getName, dataType.getPrecision),
            parametrs, measure.getFunction.getExpression))
      }.toMap.asJava
    (idToColumnMap, IDToColumn, measureId)
  }

  def extractEntityList2JavaList(cubeInstance: CubeInstance): java.util.List[LayoutEntity] = {
    extractEntityAndMeasures(cubeInstance)._1.asJava
  }

  private def toColumnDesc(ref: TblColRef, index: Int = -1) = {
    val dataType = SparkTypeUtil.toSparkType(KyDataType.getType(ref.getDatatype))
    val columnDesc = if (ref.getColumnDesc.isComputedColumn) {
      ComputedColumnDesc(ref.getName, dataType, ref.getTableRef.getTableName, ref.getTableRef.getAlias,
        index, ref.getExpressionInSourceDB)
    } else {
      ColumnDesc(ref.getName, dataType, ref.getTableRef.getTableName, ref.getTableRef.getAlias, index)
    }
    columnDesc
  }

  def extractPartitionExp(cubeSegment: CubeSegment): String = {
    val partitionDesc = cubeSegment.getModel.getPartitionDesc
    partitionDesc.setPartitionDateFormat(DateFormat.COMPACT_DATE_PATTERN)
    val (originPartitionColumn, convertedPartitionColumn) = if (partitionDesc.getPartitionDateColumnRef != null) {
      (partitionDesc.getPartitionDateColumnRef.getIdentity, convertFromDot(partitionDesc.getPartitionDateColumnRef.getIdentity))
    } else {
      (partitionDesc.getPartitionTimeColumnRef.getIdentity, convertFromDot(partitionDesc.getPartitionTimeColumnRef.getIdentity))
    }
    val originString = partitionDesc.getPartitionConditionBuilder
      .buildDateRangeCondition(partitionDesc, null, cubeSegment.getSegRange, null)
    StringUtils.replace(originString, originPartitionColumn, convertedPartitionColumn)
  }

  def extractFilterCondition(cubeSegment: CubeSegment): String = {
    val filterCondition = cubeSegment.getModel.getFilterCondition
    if (filterCondition == null) {
      ""
    } else {
      convertFromDot(cubeSegment.getModel.getFilterCondition)
    }
  }

  private val DOT_PATTERN = Pattern.compile("(\\S+)\\.(\\D+)")

  val SEPARATOR = "_0_DOT_0_"

  def convertFromDot(withDot: String): String = {
    var m = DOT_PATTERN.matcher(withDot)
    var withoutDot = withDot
    while (m.find) {
      withoutDot = m.replaceAll("$1" + SEPARATOR + "$2")
      m = DOT_PATTERN.matcher(withoutDot)
    }
    withoutDot
  }
}
