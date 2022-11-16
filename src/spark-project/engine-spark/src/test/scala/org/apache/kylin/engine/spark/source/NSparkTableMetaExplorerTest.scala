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
package org.apache.kylin.engine.spark.source

import com.google.common.collect.Sets
import org.apache.kylin.common.util.{TempMetadataBuilder, Unsafe}
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTablePartition, CatalogTableType}
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types._

class NSparkTableMetaExplorerTest extends SparderBaseFunSuite with SharedSparkSession {

  conf.set("spark.sql.catalogImplementation", "hive")
  ignore("Test load error view") {
    Unsafe.setProperty("KYLIN_CONF", TempMetadataBuilder.SPARK_PROJECT_KAP_META_TEST_DATA)
    SparderEnv.setSparkSession(spark)

    spark.sql("CREATE TABLE hive_table (a int, b int)")


    withTable("hive_table") {

      val view = CatalogTable(
        identifier = TableIdentifier("view"),
        tableType = CatalogTableType.VIEW,
        storage = CatalogStorageFormat.empty,
        schema = new StructType().add("a", "double").add("b", "int"),
        viewText = Some("SELECT 1.0 AS `a`, `b` FROM hive_table AS gen_subquery_0")
      )
      spark.sessionState.catalog.createTable(view, ignoreIfExists = false)

      withView("view") {
        val message = intercept[RuntimeException](new NSparkTableMetaExplorer().getSparkTableMeta("", "view")).getMessage
        assert(message.contains("Error for parser view: "))
      }
    }
    Unsafe.clearProperty("KYLIN_CONF")
  }

  test("Test load hive type") {
    SparderEnv.setSparkSession(spark)

    val view = CatalogTable(
      identifier = TableIdentifier("hive_table_types"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int"),
      properties = Map()
    )
    spark.sessionState.catalog.createTable(view, ignoreIfExists = false, false)

    withTable("hive_table_types") {
      val meta = new NSparkTableMetaExplorer().getSparkTableMeta("", "hive_table_types")
      assert(meta.allColumns.get(0).dataType == "char(10)")
      assert(meta.allColumns.get(1).dataType == "varchar(33)")
      assert(meta.allColumns.get(2).dataType == "int")
    }
  }

  test("Test load hive partition table") {
    SparderEnv.setSparkSession(spark)
    val table = "hive_partition_tablea";
    val view = CatalogTable(
      identifier = TableIdentifier(table),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      partitionColumnNames = List("dayno"),
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int")
        .add("dayno", "string", nullable = false, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "char(8)").build()),
      properties = Map()
    )
    spark.sessionState.catalog.createTable(view, ignoreIfExists = false)

    withTable(table) {
      val meta = new NSparkTableMetaExplorer().getSparkTableMeta("", table)
      assert(meta.allColumns.get(0).dataType == "char(10)")
      assert(meta.allColumns.get(1).dataType == "varchar(33)")
      assert(meta.allColumns.get(2).dataType == "int")
      assert(meta.partitionColumns.get(0).name.equals("dayno"))
      assert(meta.partitionColumns.get(0).dataType == "char(8)")
    }
  }

  test("Test load hive transaction table") {
    SparderEnv.setSparkSession(spark)
    val sql = "CREATE TABLE hive_transaction_table (a int, b int) clustered by (a) into 8 buckets" +
      " stored as orc TBLPROPERTIES ('transactional'='true')";
    spark.sql("DROP TABLE IF EXISTS hive_transaction_table")
    spark.sql(sql)
    val meta = new NSparkTableMetaExplorer().getSparkTableMeta("", "hive_transaction_table")
    assert(meta.allColumns.get(0).name.equals("a"))
    assert(meta.isTransactional)
  }

  test("Test get partition info from  hive partition table") {
    SparderEnv.setSparkSession(spark)
    val table = "hive_multi_partition_table";
    val view = CatalogTable(
      identifier = TableIdentifier(table),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      partitionColumnNames = List("year", "month", "day"),
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int")
        .add("year", "string", nullable = false, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "char(4)").build())
        .add("month", "string", nullable = false, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "char(2)").build())
        .add("day", "int"),
      properties = Map()
    )
    spark.sessionState.catalog.createTable(view, ignoreIfExists = false)
    spark.sessionState.catalog.createPartitions(TableIdentifier(table), Seq(CatalogTablePartition(
      Map("year" -> "2019", "month" -> "03", "day" -> "12"), CatalogStorageFormat.empty)), false)
    withTable(table) {
      assertResult(Sets.newHashSet("2019"))(new NSparkTableMetaExplorer().checkAndGetTablePartitions("", table, "year"))
      val thrown = intercept[IllegalArgumentException] {
        new NSparkTableMetaExplorer().checkAndGetTablePartitions("", table, "day")
      }
      assert(thrown.getMessage.contains("not match col"))
    }
  }

  test("Test load hive type with unsupported type array") {
    importUnsupportedCol(ArrayType(LongType))
  }

  test("Test load hive type with unsupported type map") {
    importUnsupportedCol(MapType(StringType, StringType))
  }

  test("Test load hive type with unsupported type struct") {
    importUnsupportedCol(new StructType().add("map", MapType(StringType, StringType)))
  }

  test("Test load hive type with unsupported type binary") {
    importUnsupportedCol(BinaryType)
  }

  def createTmpCatalog(st: StructType): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier("hive_table_types"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      st,
      properties = Map("role" -> "arn:aws:iam::4296365xxx:role/role-test-xxx",
        "s3_endpoint" -> "us-west-1.amazonaws.com")
    )
  }

  def importUnsupportedCol(unsupported: DataType): Unit = {
    SparderEnv.setSparkSession(spark)

    val st = new StructType()
      .add("c", "int")
      .add("d", unsupported)
    val catalogTable = createTmpCatalog(st)
    spark.sessionState.catalog.createTable(catalogTable, ignoreIfExists = false, false)

    withTable("hive_table_types") {
      val res = new NSparkTableMetaExplorer().getSparkTableMeta("", "hive_table_types")
      assert(res.allColumns.size() == 1)
      assert(res.allColumns.get(0).name.equals("c"))
      assert("arn:aws:iam::4296365xxx:role/role-test-xxx".equals(res.s3Role))
      assert("us-west-1.amazonaws.com".equals(res.s3Endpoint))
    }
  }
}
