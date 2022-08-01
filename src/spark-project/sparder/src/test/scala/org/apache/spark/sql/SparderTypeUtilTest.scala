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
package org.apache.spark.sql

import java.sql.Types

import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.sql.`type`.SqlTypeFactoryImpl
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.query.schema.OLAPTable
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.util.SparderTypeUtil

class SparderTypeUtilTest extends SparderBaseFunSuite {
  val dataTypes = List(DataType.getType("decimal(19,4)"),
    DataType.getType("char(50)"),
    DataType.getType("varchar(1000)"),
    DataType.getType("date"),
    DataType.getType("timestamp"),
    DataType.getType("tinyint"),
    DataType.getType("smallint"),
    DataType.getType("integer"),
    DataType.getType("bigint"),
    DataType.getType("float"),
    DataType.getType("double"),
    DataType.getType("decimal(38,19)"),
    DataType.getType("numeric(5,4)")
  )


  test("Test decimal") {
    val dt = DataType.getType("decimal(19,4)")
    val dataTp = DataTypes.createDecimalType(19, 4)
    val dataType = SparderTypeUtil.kylinTypeToSparkResultType(dt)
    assert(dataTp.sameType(dataType))
    val sparkTp = SparderTypeUtil.toSparkType(dt)
    assert(dataTp.sameType(sparkTp))
    val sparkTpSum = SparderTypeUtil.toSparkType(dt, true)
    assert(DataTypes.createDecimalType(29, 4).sameType(sparkTpSum))
  }

  test("Test kylinRawTableSQLTypeToSparkType") {
    dataTypes.map(SparderTypeUtil.kylinRawTableSQLTypeToSparkType)

  }

  test("Test kylinTypeToSparkResultType") {
    dataTypes.map(SparderTypeUtil.kylinTypeToSparkResultType)
  }

  test("Test toSparkType") {
    dataTypes.map(dt => {
      val sparkType = SparderTypeUtil.toSparkType(dt)
      SparderTypeUtil.convertSparkTypeToSqlType(sparkType)
    })
  }

  test("Test convertSqlTypeToSparkType") {
    val typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
    dataTypes.map(dt => {
      val relDataType = OLAPTable.createSqlType(typeFactory, dt, true)
      SparderTypeUtil.convertSqlTypeToSparkType(relDataType)
    })
  }

  test("test convertSparkFieldToJavaField") {
    val typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
    dataTypes.map(dt => {
      val relDataType = OLAPTable.createSqlType(typeFactory, dt, true)
      val structField = SparderTypeUtil.convertSparkFieldToJavaField(
        StructField("foo", SparderTypeUtil.convertSqlTypeToSparkType(relDataType))
      )

      if (relDataType.getSqlTypeName.getJdbcOrdinal == Types.CHAR) {
        assert(Types.VARCHAR == structField.getDataType)
      } else if (relDataType.getSqlTypeName.getJdbcOrdinal == Types.DECIMAL) {
        assert(Types.DECIMAL == structField.getDataType)
        assert(relDataType.getPrecision == structField.getPrecision)
        assert(relDataType.getScale == structField.getScale)
      } else {
        assert(relDataType.getSqlTypeName.getJdbcOrdinal == structField.getDataType)
      }
    })
  }

}
