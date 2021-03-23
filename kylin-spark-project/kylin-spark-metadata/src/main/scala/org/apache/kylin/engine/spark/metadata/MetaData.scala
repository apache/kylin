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

import java.util

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

class ColumnDesc(val columnName: String,
                 val dataType: DataType,
                 val tableName: String,
                 val tableAliasName: String,
                 val id: Int,
                 val rowKey: Boolean = false) extends Serializable {
  def identity: String = s"$tableAliasName.$columnName"

  def isColumnType: Boolean = true
}

object ColumnDesc {
  def apply(columnName: String, dataType: DataType, tableName: String, tableAliasName: String, id: Int, rowKey: Boolean):
  ColumnDesc = new ColumnDesc(columnName, dataType, tableName, tableAliasName, id, rowKey)
}

case class LiteralColumnDesc(override val columnName: String,
                             override val dataType: DataType,
                             override val tableName: String,
                             override val tableAliasName: String,
                             override val id: Int,
                             value: Any)
  extends ColumnDesc(columnName, dataType, tableName, tableAliasName, id) {
  override def isColumnType: Boolean = false
}

case class ComputedColumnDesc(override val columnName: String,
                              override val dataType: DataType,
                              override val tableName: String,
                              override val tableAliasName: String,
                              override val id: Int,
                              expression: String = "")
  extends ColumnDesc(columnName, dataType, tableName, tableAliasName, id)

case class TableDesc(tableName: String,
                     databaseName: String,
                     columns: List[ColumnDesc],
                     alias: String, sourceType: Int,
                     addInfo: util.Map[String, String]) {
  def identity: String = s"$databaseName.$tableName"

  def toSchema: StructType = {
    StructType(columns.map(column => StructField(column.columnName, column.dataType)))
  }
}

case class FunctionDesc(functionName: String, returnType: DTType, pra: List[ColumnDesc], expression: String) {
  override def toString: String = {
    s"$functionName par=${pra.map(_.columnName).mkString(",")} dt=$returnType"
  }
}

case class DTType(dataType: String, precision: Int, scale: Int) {
  def toKylinDataType: org.apache.kylin.metadata.datatype.DataType = {
    if (precision != -1 && scale != -1) {
      org.apache.kylin.metadata.datatype.DataType.getType(s"$dataType($precision, $scale)")
    } else if (precision != -1 && scale == -1) {
      org.apache.kylin.metadata.datatype.DataType.getType(s"$dataType($precision)")
    } else {
      org.apache.kylin.metadata.datatype.DataType.getType(s"$dataType")
    }
  }

  override def toString: String = s"$dataType($precision, $scale)"
}

case class JoinDesc(lookupTable: TableDesc, PKS: Array[ColumnDesc], FKS: Array[ColumnDesc], joinType: String)

case class SegmentInfo(id: String,
                       name: String,
                       identifier: String,
                       project: String,
                       kylinconf: KylinConfig,
                       factTable: TableDesc,
                       lookupTables: List[TableDesc],
                       snapshotTables: List[TableDesc],
                       joindescs: Array[JoinDesc],
                       allColumns: List[ColumnDesc],
                       layouts: List[LayoutEntity],
                       var toBuildLayouts: mutable.Set[LayoutEntity],
                       var toBuildDictColumns: Set[ColumnDesc],
                       allDictColumns: Set[ColumnDesc],
                       partitionExp: String,
                       filterCondition: String,
                       allRowKeyCols: List[ColumnDesc],
                       var snapshotInfo: Map[String, String] = Map.empty[String, String]) {

  def updateLayout(layoutEntity: LayoutEntity): Unit = {
    toBuildLayouts.remove(layoutEntity)
  }

  def removeLayout(layoutId: Long): Unit = {
    toBuildLayouts = toBuildLayouts.filter(layout => !layout.getId.equals(layoutId))
  }

  def updateSnapshot(tableInfo: Map[String, String]): Unit = {
    snapshotInfo = tableInfo
  }

  def getAllLayoutSize: Long = {
    layouts.map(_.getByteSize).sum
  }

  def getAllLayout: List[LayoutEntity] = {
    layouts
  }

  def getAllLayoutJava: java.util.List[LayoutEntity] = {
    val l: util.LinkedList[LayoutEntity] = new java.util.LinkedList()
    layouts.foreach(o => l.add(o))
    l
  }

  def getSnapShot2JavaMap: java.util.Map[String, String] = {
    snapshotInfo.asJava
  }

  override def toString: String = s"${id}_${name}_$identifier"
}
