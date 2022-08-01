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
package org.apache.spark.sql.newSession

import org.apache.kylin.common.util.TempMetadataBuilder

import java.util.Locale
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest
import org.apache.kylin.engine.spark.source.{NSparkMetadataExplorer, NSparkTableMetaExplorer}
import org.apache.kylin.metadata.model.NTableMetadataManager
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
/**
 * Equivalence [[org.apache.kylin.engine.spark.source.NSparkTableMetaExplorerTest]] +
 * [[org.apache.kylin.engine.spark.source.NSparkMetadataExplorerTest]]
 */
class MetaExplorerTest extends SQLTestUtils with WithKylinExternalCatalog {

  val table : String = "tableX"
  // import org.apache.spark.sql.newSession.TestKylin.implicits._

  test("Test load error view") {
    spark.sql(s"CREATE TABLE $table (a int, b int) using csv")

    val view = CatalogTable(
      identifier = TableIdentifier("view"),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", "double").add("b", "int"),
      viewText = Some(s"SELECT 1.0 AS `a`, `b` FROM $table AS gen_subquery_0")
    )
    spark.sessionState.catalog.createTable(view, ignoreIfExists = false)

    withTable(table) {
      withView("view") {
        val message =
          intercept[RuntimeException](new NSparkTableMetaExplorer().getSparkTableMeta("", "view"))
            .getMessage
        assert(message.contains("Error for parser view: "))
      }
    }
  }
  test("Test load hive type") {
    val view = CatalogTable(
      identifier = TableIdentifier(table),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int"),
      properties = Map()
    )
    spark.sessionState.catalog.createTable(view, ignoreIfExists = false)
    withTable(table) {
      val meta = new NSparkTableMetaExplorer().getSparkTableMeta("", table)
      assertResult("char(10)")(meta.getAllColumns.get(0).getDataType)
      assertResult("varchar(33)")(meta.getAllColumns.get(1).getDataType)
      assertResult("int")(meta.getAllColumns.get(2).getDataType)
    }
  }

  def createTmpCatalog(table: String, st: StructType): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier(table),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      st,
      properties = Map()
    )
  }
  def importUnsupportedCol(table: String, unsupported: DataType): Unit = {
    SparderEnv.setSparkSession(spark)

    val st = new StructType()
      .add("c", "int")
      .add("d", unsupported)
    val catalogTable = createTmpCatalog(table, st)
    spark.sessionState.catalog.createTable(catalogTable, ignoreIfExists = false)

    withTable(table) {
      val res = new NSparkTableMetaExplorer().getSparkTableMeta("", table)
      assert(res.getAllColumns.size() == 1)
      assert(res.getAllColumns.get(0).getName.equals("c"))
    }
  }

  test("Test load hive type with unsupported type array") {
    importUnsupportedCol(table, ArrayType(LongType))
  }


  test("Test load hive type with unsupported type map") {
    importUnsupportedCol(table, MapType(StringType, StringType))
  }

  test("Test load hive type with unsupported type struct") {
    importUnsupportedCol(table, new StructType().add("map", MapType(StringType, StringType)))
  }

  test("Test load hive type with unsupported type binary") {
    importUnsupportedCol(table, BinaryType)
  }

  test("ListDatabases") {
    val sparkMetadataExplorer: NSparkMetadataExplorer = new NSparkMetadataExplorer
    val databases = sparkMetadataExplorer.listDatabases
    assertResult(4)(databases.size())
    assertResult(Seq("DEFAULT", "EDW", "FILECATALOGUT", "SSB"))(databases.asScala)
  }

  test("ListTables") {
    spark.sql(s"CREATE TABLE $table (a int, b int) using csv")
    withTable(table) {
      val sparkMetadataExplorer = new NSparkMetadataExplorer
      val tables = sparkMetadataExplorer.listTables("")
      assertResult(12)(tables.size())
      assertResult(
        Seq("STREAMING_TABLE", s"${table.toUpperCase(Locale.ROOT)}", "TEST_ACCOUNT", "TEST_CATEGORY_GROUPINGS", "TEST_COUNTRY",
          "TEST_ENCODING", "TEST_KYLIN_FACT", "TEST_KYLIN_FACT_WITH_INT_DATE", "TEST_MEASURE", "TEST_MEASURE1",
          "TEST_ORDER", "TEST_SCD2"))(tables.asScala)
    }
  }

  test("testListTablesInDatabase") {
    val testDataBase = "SSB"
    val sparkMetadataExplorer = new NSparkMetadataExplorer
    assert(spark.catalog.databaseExists(testDataBase))
    val tables = sparkMetadataExplorer.listTables(testDataBase)
    assert(tables != null && tables.size() > 0)
    assertResult(Seq("CUSTOMER", "DATES", "LINEORDER", "PART", "P_LINEORDER", "SUPPLIER"))(tables.asScala)
  }

  test("testGetTableDesc") {
    NLocalWithSparkSessionTest.populateSSWithCSVData(kylinConf, "ssb", spark)
    val sparkMetadataExplorer = new NSparkMetadataExplorer
    val tableDescTableExtDescPair =
      sparkMetadataExplorer.loadTableMetadata("", "p_lineorder", "ssb")
    assert(tableDescTableExtDescPair != null && tableDescTableExtDescPair.getFirst != null)
  }

  test("testCreateSampleDatabase") {
    val sparkMetadataExplorer = new NSparkMetadataExplorer
    sparkMetadataExplorer.createSampleDatabase("TEST")
    val databases = sparkMetadataExplorer.listDatabases
  assert(databases != null && databases.contains("TEST"))
  }

  test("testCreateSampleTable") {
    val sparkMetadataExplorer = new NSparkMetadataExplorer
    val tableMgr = NTableMetadataManager.getInstance(kylinConf, "default")
    val fact = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT")
    fact.setName("TEST_KYLIN_FACT2"); // Already existed in FileCatalog, so let's rename it.
    sparkMetadataExplorer.createSampleTable(fact)
    val tables = sparkMetadataExplorer.listTables("default")
    assert(tables != null && tables.contains("TEST_KYLIN_FACT2"))
  }

  test("testLoadData") {
    val sparkMetadataExplorer = new NSparkMetadataExplorer
    sparkMetadataExplorer.loadSampleData("SSB.PART", TempMetadataBuilder.TEMP_TEST_METADATA + "/data/")
    val rows = spark.sql("select * from part").collectAsList
    assert(rows != null && rows.size > 0)
  }
}
