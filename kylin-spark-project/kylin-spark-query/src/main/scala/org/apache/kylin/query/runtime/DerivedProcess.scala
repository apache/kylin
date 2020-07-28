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
package org.apache.kylin.query.runtime

import java.util

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.cube.cuboid.Cuboid
import org.apache.kylin.cube.CubeInstance
import org.apache.kylin.cube.model.CubeDesc.{DeriveInfo, DeriveType}
import org.apache.kylin.metadata.model.{JoinDesc, TblColRef}
import org.apache.kylin.metadata.TableMetadataManager
import org.apache.kylin.query.relnode.OLAPContext
import org.apache.kylin.query.{DeriveTableColumnInfo, SchemaProcessor}
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object DerivedProcess {

  def process(
    olapContext: OLAPContext, cuboid: Cuboid, cubeInstance: CubeInstance, dataFrame: DataFrame,
    request: GTCubeStorageQueryRequest): (DataFrame, DeriveSummary) = {
    val tupleInfo = olapContext.returnTupleInfo
    val gridTableMapping = cuboid.getCuboidToGridTableMapping
    val columnIndex = gridTableMapping.getDimIndexes(request.getDimensions) ++
      gridTableMapping.getMetricsIndexes(request.getMetrics)
    var hasDerived = false
    val alias = olapContext.firstTableScan.getBackupAlias
    var hostToDerives: List[DeriveData] = List.empty
    val derivedColumnNameMapping: java.util.HashMap[DeriveData, Array[String]] = new util.HashMap[DeriveData, Array[String]]()

    def findDerivedColumn(hostCols: Array[TblColRef], deriveInfo: DeriveInfo): Unit = {
      //  for PK_FK derive on composite join keys, hostCols may be incomplete
      //  see CubeDesc.initDimensionColumns()
      var hostFkCols: Array[TblColRef] = null
      if (deriveInfo.`type` == DeriveType.PK_FK) {
        //  if(false)
        if (deriveInfo.join.getForeignKeyColumns.contains(hostCols.apply(0))) {
          // FK -> PK, most cases
          hostFkCols = deriveInfo.join.getForeignKeyColumns
        } else {
          hostFkCols = deriveInfo.join.getPrimaryKeyColumns
        }
      } else {
        hostFkCols = hostCols
      }

      val hostFkIdx = hostFkCols.map(hostCol => indexOnTheGTValues(hostCol))
      if (!hostFkIdx.exists(_ >= 0)) {
        return
      }

      val hostIndex = hostCols.map(hostCol => indexOnTheGTValues(hostCol))
      if (hostIndex.exists(_ < 0)) {
        return
      }

      val calciteIdx = deriveInfo.columns
        .map(column =>
          if (tupleInfo.hasColumn(column)) {
            tupleInfo.getColumnIndex(column)
          } else {
            -1
          })
        .filter(_ >= 0)

      if (calciteIdx.isEmpty) {
        return
      }

      val (path, tableName, aliasTableName, pkIndex) =
        getLookupTablePathAndPkIndex(deriveInfo, cubeInstance, alias)

      val lookupIdx = deriveInfo.columns.map(_.getColumnDesc.getZeroBasedIndex)
      hostToDerives ++= List(
        DeriveData(hostIndex,
          hostFkIdx,
          pkIndex,
          calciteIdx,
          lookupIdx,
          path,
          tableName,
          aliasTableName,
          deriveInfo.join,
          deriveInfo.`type`))
      hasDerived = true
    }

    def indexOnTheGTValues(col: TblColRef): Int = {
      val cuboidDims = cuboid.getColumns
      val cuboidIdx = cuboidDims.indexOf(col)
      if (columnIndex.contains(cuboidIdx)) {
        cuboidIdx
      } else {
        -1
      }
    }

    def joinLookUpTable(
      gTInfoNames: Array[String], df: DataFrame,
      derivedInfo: DeriveData, kylinConfig: KylinConfig): DataFrame = {
      val lookupTableAlias = derivedInfo.aliasTableName

      val lookupDf =
        SparderLookupManager.getOrCreate(derivedInfo.tableIdentity,
          derivedInfo.path,
          kylinConfig)

      val newNames = lookupDf.schema.fieldNames
        .map { name =>
          SchemaProcessor.parseDeriveTableSchemaName(name)

        }
        .sortBy(_.columnId)
        .map(
          deriveInfo =>
            DeriveTableColumnInfo(lookupTableAlias,
              deriveInfo.columnId,
              deriveInfo.columnName).toString)
        .array
      derivedColumnNameMapping.put(derivedInfo, newNames)
      val newNameLookupDf = lookupDf.toDF(newNames: _*)
      if (derivedInfo.fkIdx.length != derivedInfo.pkIdx.length) {
        throw new IllegalStateException(
          s"unequal host key num ${derivedInfo.fkIdx.length} " +
            s"vs derive pk num ${derivedInfo.pkIdx.length} ")
      }
      val zipIndex = derivedInfo.fkIdx.zip(derivedInfo.pkIdx)
      var joinCol: Column = null
      zipIndex.foreach {
        case (hostIndex, pkIndex) =>
          if (joinCol == null) {
            joinCol = col(gTInfoNames.apply(hostIndex))
              .equalTo(col(newNames(pkIndex)))
          } else {
            joinCol = joinCol.and(
              col(gTInfoNames.apply(hostIndex))
                .equalTo(col(newNames(pkIndex))))
          }
      }
      df.join(newNameLookupDf, joinCol, derivedInfo.join.getType)
    }

    cubeInstance.getDescriptor.getHostToDerivedInfo(cuboid.getColumns, null)
      .asScala.toList
      .flatMap(pair => {
        pair._2.asScala.map(di => (pair._1, di))
      })
      .sortBy(p => p._1.data.length)
      .reverse
      .foreach(entry => findDerivedColumn(entry._1.data, entry._2))
    var joinedDf: DataFrame = dataFrame
    val joinedLookups = scala.collection.mutable.Set[String]()

    for (hostToDerived <- hostToDerives) {
      if (hostToDerived.deriveType != DeriveType.PK_FK) {
        //  PK_FK derive does not need joining
        if (!joinedLookups.contains(hostToDerived.aliasTableName)) {
          joinedDf = joinLookUpTable(dataFrame.schema.fieldNames,
            joinedDf,
            hostToDerived,
            cubeInstance.getConfig)
          joinedLookups.add(hostToDerived.aliasTableName)
        }
      }
    }
    (joinedDf, DeriveSummary(hostToDerives, derivedColumnNameMapping))
  }

  def getLookupTablePathAndPkIndex(deriveInfo: DeriveInfo, cubeInstance: CubeInstance, alias: String):
  (String, String, String, Array[Int]) = {
    val segment = cubeInstance.getLatestReadySegment
    val join = deriveInfo.join
    val metaMgr = TableMetadataManager.getInstance(segment.getConfig)
    val derivedTableName = join.getPKSide.getTableIdentity
    val pkCols = join.getPrimaryKey
    val tableDesc = metaMgr.getTableDesc(derivedTableName, cubeInstance.getProject)
    val pkIndex =
      pkCols.map(pkCol => tableDesc.findColumnByName(pkCol).getZeroBasedIndex)
    val path = segment.getSnapshotResPath(derivedTableName)
    if (path == null && deriveInfo.`type` != DeriveType.PK_FK) {
      throw new IllegalStateException(
        "No snapshot for table '" + derivedTableName + "' found on cube segment"
          + cubeInstance.getName + "/" + segment)
    }
    (path, cubeInstance.getProject + "@" + derivedTableName, s"${alias}_${join.getPKSide.getAlias}", pkIndex)
  }

}

case class DeriveData(
  hostIdx: Array[Int],
  fkIdx: Array[Int],
  pkIdx: Array[Int],
  calciteIdx: Array[Int],
  derivedIndex: Array[Int],
  path: String,
  tableIdentity: String,
  aliasTableName: String,
  join: JoinDesc,
  deriveType: DeriveType) {
  require(fkIdx.length == pkIdx.length)
  require(calciteIdx.length == derivedIndex.length)
}

case class DeriveSummary(deriveDataList: List[DeriveData], deviceMapping: java.util.HashMap[DeriveData, Array[String]])