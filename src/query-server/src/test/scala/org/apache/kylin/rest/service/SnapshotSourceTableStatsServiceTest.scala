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

package org.apache.kylin.rest.service

import org.apache.kylin.guava30.shaded.common.collect.{Lists, Maps, Sets}
import org.apache.commons.collections4.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.constant.Constants
import org.apache.kylin.common.util.{HadoopUtil, RandomUtil}
import org.apache.kylin.job.snapshot.SnapshotJobUtils
import org.apache.kylin.metadata.model.{NTableMetadataManager, TableDesc}
import org.apache.kylin.rest.model.SnapshotSourceTableStats
import org.apache.kylin.rest.scheduler.AutoRefreshSnapshotRunner
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.awaitility.Awaitility.await
import org.awaitility.Duration
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.mockito.Mockito.{mock, mockStatic, when}
import org.mockito.{ArgumentMatchers, Mock, MockedStatic, Mockito}

import java.io.IOException
import java.util
import java.util.Locale
import scala.collection.JavaConverters._

class SnapshotSourceTableStatsServiceTest extends SparderBaseFunSuite with LocalMetadata with SharedSparkSession {
  private val DEFAULT_PROJECT = "default"
  conf.set("spark.sql.catalogImplementation", "hive")
  @Mock
  private val snapshotSourceTableStatsService = Mockito.spy(classOf[SnapshotSourceTableStatsService])

  override def beforeAll(): Unit = {
    super.beforeAll()
    writeMarkFile()
  }

  private def writeMarkFile(): Unit = {
    val fs = HadoopUtil.getWorkingFileSystem
    val markFile = new Path(KylinConfig.getInstanceFromEnv.getSnapshotAutoRefreshDir(DEFAULT_PROJECT) + Constants.MARK)
    try {
      val out = fs.create(markFile, true)
      try out.write(new String().getBytes())
      catch {
        case e: IOException =>
          log.error(s"overwrite mark file [$markFile] failed!", e)
      } finally if (out != null) out.close()
    }
  }

  override def afterAll(): Unit = {
    try {
      deleteMarkFile()
    } finally {
      super.afterAll()
    }
  }

  private def deleteMarkFile(): Unit = {
    val markFile = new Path(KylinConfig.getInstanceFromEnv.getSnapshotAutoRefreshDir(DEFAULT_PROJECT) + Constants.MARK)
    val fs = HadoopUtil.getWorkingFileSystem
    if (fs.exists(markFile)) {
      fs.delete(markFile, true)
    }
  }

  def hiveTable(table: String, f: (KylinConfig, SessionCatalog, CatalogTable) => Unit): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    SparderEnv.setSparkSession(spark)
    val view = CatalogTable(
      identifier = TableIdentifier(table, Option.apply("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int"),
      properties = Map()
    )
    val catalog = spark.sessionState.catalog
    catalog.createTable(view, ignoreIfExists = false, validateLocation = false)
    spark.sql("insert into default." + table + " values(\"1\",\"1\",1)")
    spark.sql("insert into default." + table + " values(\"2\",\"2\",2)")

    withTable(table) {
      val temp = spark.sessionState.catalog.getTempViewOrPermanentTableMetadata(view.identifier)
      f(config, catalog, temp)
    }
  }

  def hivePartitionTable(table: String, f: (KylinConfig, SessionCatalog, CatalogTable) => Unit): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    SparderEnv.setSparkSession(spark)
    val view = CatalogTable(
      identifier = TableIdentifier(table, Option.apply("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      partitionColumnNames = List("year", "month", "day"),
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int")
        .add("year", "string", nullable = false, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(4)").build())
        .add("month", "string", nullable = false, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(2)").build())
        .add("day", "int"),
      properties = Map()
    )
    val catalog = spark.sessionState.catalog
    catalog.createTable(view, ignoreIfExists = false)
    catalog.createPartitions(TableIdentifier(table), Seq(CatalogTablePartition(
      Map("year" -> "2019", "month" -> "03", "day" -> "12"), CatalogStorageFormat.empty)), false)
    spark.sql("insert into default." + table + " PARTITION (year=\"1\",month=\"1\",day=1) values(\"1\",\"1\",1)")
    spark.sql("insert into default." + table + " PARTITION (year=\"1\",month=\"1\",day=1) values(\"11\",\"11\",11)")
    await.pollDelay(Duration.ONE_SECOND).until(() => true)
    spark.sql("insert into default." + table + " PARTITION (year=\"2\",month=\"2\",day=2) values(\"2\",\"2\",2)")
    spark.sql("insert into default." + table + " PARTITION (year=\"2\",month=\"2\",day=2) values(\"22\",\"22\",22)")

    withTable(table) {
      val temp = spark.sessionState.catalog.getTempViewOrPermanentTableMetadata(view.identifier)
      f(config, catalog, temp)
    }
  }


  private def init(tableName: String, database: String)(f: => Unit): Unit = {
    var mockedStatic: MockedStatic[NTableMetadataManager] = null
    try {
      mockedStatic = mockStatic(classOf[NTableMetadataManager])
      val tableMetadataManager = mock(classOf[NTableMetadataManager])
      mockedStatic.when(() =>
        NTableMetadataManager.getInstance(ArgumentMatchers.any[KylinConfig], ArgumentMatchers.any[String]))
        .thenReturn(tableMetadataManager)

      val table = mock(classOf[TableDesc])
      when(table.getProject).thenReturn(DEFAULT_PROJECT)
      when(table.getDatabase).thenReturn(database)
      when(table.getName).thenReturn(tableName)
      when(table.getIdentity).thenReturn(database + "." + tableName)
      when(tableMetadataManager.getTableDesc(ArgumentMatchers.any[String])).thenReturn(table)

      f
    } finally {
      if (mockedStatic != null) {
        mockedStatic.close()
      }
    }
  }

  private def writeEmptyJsonFile(tableIdentity: String): Unit = {
    val fs = HadoopUtil.getWorkingFileSystem
    val statsFile = new Path(KylinConfig.getInstanceFromEnv.getSnapshotAutoRefreshDir(DEFAULT_PROJECT)
      + Constants.SOURCE_TABLE_STATS + "/" + tableIdentity)
    try {
      val out = fs.create(statsFile, true)
      try out.write(new String("{}").getBytes())
      catch {
        case e: IOException =>
          log.error(s"overwrite stats file [$statsFile] failed!", e)
      } finally if (out != null) out.close()
    }
  }

  test("getSnapshotSourceTables - hive table") {
    val tableName = "hive_table_types" + RandomUtil.randomUUIDStr().replace("-", "_")
    hiveTable(tableName, (config, catalog, table) => {
      init(tableName, table.database) {
        val tableIdentity = table.qualifiedName.toLowerCase(Locale.ROOT)
        val locationPath = table.location.getPath
        val locationFilesStatus: util.List[FileStatus] = snapshotSourceTableStatsService.getLocationFileStatus(locationPath)
        val snapshotTablesLocationsJson = snapshotSourceTableStatsService.createSnapshotSourceTableStats(locationPath, config,
          locationFilesStatus)
        snapshotSourceTableStatsService.writeSourceTableStats(DEFAULT_PROJECT, tableIdentity, snapshotTablesLocationsJson)

        val fromJson = snapshotSourceTableStatsService.getSnapshotSourceTableStatsJsonFromHDFS(DEFAULT_PROJECT, tableIdentity).getSecond
        assertEquals(snapshotTablesLocationsJson.size(), fromJson.size())
        fromJson.forEach((key, actual) => {
          val expected = snapshotTablesLocationsJson.get(key)
          assertEquals(expected.getFilesSize, actual.getFilesSize)
          assertEquals(expected.getCreateTime, actual.getCreateTime)
          assertEquals(expected.getFilesModificationTime, actual.getFilesModificationTime)
          assertEquals(expected.getFilesCount, actual.getFilesCount)
        })

        val locationFilesStatus2: util.List[FileStatus] = Lists.newArrayList()
        val needCheck = snapshotSourceTableStatsService.checkLocation(locationPath, locationFilesStatus2, fromJson, config)
        assertFalse(needCheck)
      }
    })
  }

  test("getSnapshotSourceTables - hive table check-location") {
    val tableName = "hive_table_types" + RandomUtil.randomUUIDStr().replace("-", "_")
    hiveTable(tableName, (config, catalog, table) => {
      val locationPath = table.location.getPath
      val fromJson: util.Map[String, SnapshotSourceTableStats] = Maps.newHashMap()
      val locationFilesStatus: util.List[FileStatus] = Lists.newArrayList()
      val needCheck = snapshotSourceTableStatsService.checkLocation(locationPath, locationFilesStatus, fromJson, config)
      assertTrue(needCheck)

      val tableIdentity = table.qualifiedName.toLowerCase(Locale.ROOT)
      writeEmptyJsonFile(tableIdentity)
      writeMarkFile()
      val result = snapshotSourceTableStatsService.checkHiveTable(DEFAULT_PROJECT, table, config, tableIdentity)
      assertTrue(result)
    })
  }

  test("checkSourceTableStats - hive table") {
    val tableName = "hive_table_types" + RandomUtil.randomUUIDStr().replace("-", "_")
    hiveTable(tableName, (config, catalog, table) => {
      val tableIdentity = table.qualifiedName.toLowerCase(Locale.ROOT)
      writeEmptyJsonFile(tableIdentity)
      writeMarkFile()
      val response = snapshotSourceTableStatsService.checkSourceTableStats(DEFAULT_PROJECT, table.database, table.identifier.table, null)
      assertTrue(response.getNeedRefresh)
      assertTrue(CollectionUtils.isEmpty(response.getNeedRefreshPartitionsValue))
    })
  }

  test("checkSourceTableStats - hive table - no json") {
    val tableName = "hive_table_types" + RandomUtil.randomUUIDStr().replace("-", "_")
    hiveTable(tableName, (config, catalog, table) => {
      val response = snapshotSourceTableStatsService.checkSourceTableStats(DEFAULT_PROJECT, table.database, table.identifier.table, null)
      assertFalse(response.getNeedRefresh)
      assertTrue(CollectionUtils.isEmpty(response.getNeedRefreshPartitionsValue))
    })
  }

  test("getSnapshotSourceTables - hive partition table") {
    val tableName = "hive_multi_partition_table" + RandomUtil.randomUUIDStr().replace("-", "_")
    hivePartitionTable(tableName, (config, catalog, table) => {
      init(tableName, table.database) {
        val tableIdentity = table.qualifiedName.toLowerCase(Locale.ROOT)

        val snapshotTablesLocationsJson = Maps.newHashMap[String, SnapshotSourceTableStats]()
        val needSavePartitionsFilesStatus = Maps.newHashMap[String, util.List[FileStatus]]()
        val partitions = catalog.listPartitions(table.identifier, Option.empty).asJava
        val needCheckPartitions = partitions.asScala.sortBy(partition => partition.createTime).reverse
          .slice(0, config.getSnapshotAutoRefreshFetchPartitionsCount).asJava

        snapshotSourceTableStatsService.putNeedSavePartitionsFilesStatus(needCheckPartitions, needSavePartitionsFilesStatus)
        for (partition <- partitions.asScala) {
          snapshotSourceTableStatsService.createPartitionSnapshotSourceTableStats(partition, needSavePartitionsFilesStatus,
            snapshotTablesLocationsJson, config)
        }
        snapshotSourceTableStatsService.writeSourceTableStats(DEFAULT_PROJECT, tableIdentity, snapshotTablesLocationsJson)

        val fromJson = snapshotSourceTableStatsService.getSnapshotSourceTableStatsJsonFromHDFS(DEFAULT_PROJECT, tableIdentity).getSecond
        assertEquals(snapshotTablesLocationsJson.size(), fromJson.size())
        fromJson.forEach((key, actual) => {
          val expected = snapshotTablesLocationsJson.get(key)
          assertEquals(expected.getFilesSize, actual.getFilesSize)
          assertEquals(expected.getCreateTime, actual.getCreateTime)
          assertEquals(expected.getFilesModificationTime, actual.getFilesModificationTime)
          assertEquals(expected.getFilesCount, actual.getFilesCount)
        })

        val needRefreshPartitions: util.List[CatalogTablePartition] = Lists.newArrayList()
        val needSavePartitionsFilesStatus2 = Maps.newHashMap[String, util.List[FileStatus]]()
        val needCheck = snapshotSourceTableStatsService.checkPartitionsLocation(partitions, fromJson, needRefreshPartitions,
          needSavePartitionsFilesStatus2, config)
        assertFalse(needCheck)
        assertTrue(CollectionUtils.isEmpty(needRefreshPartitions))
        assertTrue(MapUtils.isNotEmpty(needSavePartitionsFilesStatus2))
        needSavePartitionsFilesStatus2.forEach((key, actual) => {
          val expected = needSavePartitionsFilesStatus.get(key)
          assertEquals(expected.size(), actual.size())
          for (i <- 0 until actual.size()) {
            assertEquals(expected.get(i).getLen, actual.get(i).getLen)
            assertEquals(expected.get(i).getModificationTime, actual.get(i).getModificationTime)
          }
        })
      }
    })
  }

  test("getSnapshotSourceTables - hive partition table check-partition-hive-table") {
    val tableName = "hive_multi_partition_table" + RandomUtil.randomUUIDStr().replace("-", "_")
    hivePartitionTable(tableName, (config, catalog, table) => {
      val tableIdentity = table.qualifiedName.toLowerCase(Locale.ROOT)
      writeEmptyJsonFile(tableIdentity)
      writeMarkFile()
      val needRefreshPartitions: util.List[CatalogTablePartition] = Lists.newArrayList()
      val needCheck = snapshotSourceTableStatsService.checkPartitionHiveTable(DEFAULT_PROJECT, catalog, table,
        needRefreshPartitions, config, tableIdentity)
      assertTrue(needCheck)
      assertTrue(CollectionUtils.isNotEmpty(needRefreshPartitions))

      val partitions = catalog.listPartitions(table.identifier, Option.empty).sortBy(partition => partition.createTime)
        .reverse.slice(0, config.getSnapshotAutoRefreshFetchPartitionsCount)
      assertEquals(partitions.length, needRefreshPartitions.size())
      for (partition <- partitions) {
        val sameCount = needRefreshPartitions.stream()
          .filter(par => StringUtils.equals(par.location.getPath, partition.location.getPath))
          .filter(par => par.createTime == partition.createTime).count()
        assertEquals(1, sameCount)
      }
    })
  }

  test("checkSourceTableStats - hive partition table") {
    val tableName = "hive_multi_partition_table" + RandomUtil.randomUUIDStr().replace("-", "_")
    hivePartitionTable(tableName, (config, catalog, table) => {
      val tableIdentity = table.qualifiedName.toLowerCase(Locale.ROOT)
      writeEmptyJsonFile(tableIdentity)
      writeMarkFile()
      val response = snapshotSourceTableStatsService.checkSourceTableStats(DEFAULT_PROJECT, table.database, table.identifier.table, "year")
      val needRefreshPartitionsValue = response.getNeedRefreshPartitionsValue
      assertTrue(response.getNeedRefresh)
      assertTrue(CollectionUtils.isNotEmpty(needRefreshPartitionsValue))
      assertEquals(1, needRefreshPartitionsValue.size())
      assertTrue(needRefreshPartitionsValue.contains("2   "))
    })
  }

  test("checkSourceTableStats - hive partition table - no json") {
    val tableName = "hive_multi_partition_table" + RandomUtil.randomUUIDStr().replace("-", "_")
    hivePartitionTable(tableName, (config, catalog, table) => {
      val response = snapshotSourceTableStatsService.checkSourceTableStats(DEFAULT_PROJECT, table.database, table.identifier.table, "year")
      val needRefreshPartitionsValue = response.getNeedRefreshPartitionsValue
      assertFalse(response.getNeedRefresh)
      assertTrue(CollectionUtils.isEmpty(needRefreshPartitionsValue))
    })
  }

  test("saveSnapshotViewMapping") {
    val hiveTableName = "hive_table_types" + RandomUtil.randomUUIDStr().replace("-", "_")
    SparderEnv.setSparkSession(spark)
    val hiveTable = CatalogTable(
      identifier = TableIdentifier(hiveTableName, Option.apply("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int"),
      properties = Map()
    )
    val catalog = spark.sessionState.catalog
    catalog.createTable(hiveTable, ignoreIfExists = false, validateLocation = false)

    val hiveMultiPartitionTableName = "hive_multi_partition_table" + RandomUtil.randomUUIDStr().replace("-", "_")
    val hiveMultiPartitionTable = CatalogTable(
      identifier = TableIdentifier(hiveMultiPartitionTableName, Option.apply("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      partitionColumnNames = List("year", "month", "day"),
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int")
        .add("year", "string", nullable = false, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(4)").build())
        .add("month", "string", nullable = false, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(2)").build())
        .add("day", "int"),
      properties = Map()
    )
    catalog.createTable(hiveMultiPartitionTable, ignoreIfExists = false)

    val viewName = "view_" + RandomUtil.randomUUIDStr().replace("-", "_")
    val view = CatalogTable(
      identifier = TableIdentifier(viewName, Option.apply("default")),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", "string").add("b", "string"),
      viewText = Some("SELECT t1.*, t2.* FROM " + hiveTable.qualifiedName + " t1 join " + hiveMultiPartitionTable.qualifiedName
        + " t2 on t1.a = t2.b")
    )
    catalog.createTable(view, ignoreIfExists = false)

    withTable(hiveTableName, hiveMultiPartitionTableName) {
      withView(viewName) {
        var mockedStatic: MockedStatic[SnapshotJobUtils] = null
        try {
          mockedStatic = mockStatic(classOf[SnapshotJobUtils])
          val table = mock(classOf[TableDesc])
          when(table.getName).thenReturn(view.identifier.table)
          when(table.getDatabase).thenReturn(view.database)
          when(table.getIdentity).thenReturn(view.qualifiedName)
          when(table.isView).thenReturn(true)
          mockedStatic.when(() =>
            SnapshotJobUtils.getSnapshotTables(ArgumentMatchers.any[KylinConfig], ArgumentMatchers.any[String])
          ).thenReturn(Lists.newArrayList(table))
          val result = snapshotSourceTableStatsService.saveSnapshotViewMapping(DEFAULT_PROJECT)
          assertTrue(result)

          val runner = AutoRefreshSnapshotRunner.getInstance(DEFAULT_PROJECT)
          val actual = runner.readViewTableMapping
          assertEquals(1, actual.size())
          assertTrue(actual.keySet().contains(table.getIdentity))
          val sourceTable = actual.get(table.getIdentity)
          assertEquals(2, sourceTable.size())
          assertTrue(sourceTable.containsAll(Sets.newHashSet(hiveTable.qualifiedName, hiveMultiPartitionTable.qualifiedName)))
        } finally {
          if (mockedStatic != null) {
            mockedStatic.close()
          }
          AutoRefreshSnapshotRunner.shutdown(DEFAULT_PROJECT)
        }
      }
    }
  }

  test("saveSnapshotViewMapping - not view") {
    val hiveTableName = "hive_table_types" + RandomUtil.randomUUIDStr().replace("-", "_")
    SparderEnv.setSparkSession(spark)
    val hiveTable = CatalogTable(
      identifier = TableIdentifier(hiveTableName, Option.apply("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder()
          .putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(33)").build())
        .add("c", "int"),
      properties = Map()
    )
    val catalog = spark.sessionState.catalog
    catalog.createTable(hiveTable, ignoreIfExists = false, validateLocation = false)
    withTable(hiveTableName) {
      var mockedStatic: MockedStatic[SnapshotJobUtils] = null
      try {
        mockedStatic = mockStatic(classOf[SnapshotJobUtils])
        val table = mock(classOf[TableDesc])
        when(table.getName).thenReturn(hiveTable.identifier.table)
        when(table.getDatabase).thenReturn(hiveTable.database)
        when(table.getIdentity).thenReturn(hiveTable.qualifiedName)
        mockedStatic.when(() =>
          SnapshotJobUtils.getSnapshotTables(ArgumentMatchers.any[KylinConfig], ArgumentMatchers.any[String])
        ).thenReturn(Lists.newArrayList(table))
        val result = snapshotSourceTableStatsService.saveSnapshotViewMapping(DEFAULT_PROJECT)
        assertTrue(result)

        val runner = AutoRefreshSnapshotRunner.getInstance(DEFAULT_PROJECT)
        val actual = runner.readViewTableMapping
        assertEquals(0, actual.size())
      } finally {
        if (mockedStatic != null) {
          mockedStatic.close()
        }
        AutoRefreshSnapshotRunner.shutdown(DEFAULT_PROJECT)
      }
    }
  }

  test("checkSnapshotSourceTableStatsJsonFile - delete mark file") {
    val tableName = "hive_table_types" + RandomUtil.randomUUIDStr().replace("-", "_")
    hiveTable(tableName, (config, catalog, table) => {
      val tableIdentity = table.qualifiedName.toLowerCase(Locale.ROOT)
      writeEmptyJsonFile(tableIdentity)
      writeMarkFile()
      await.pollDelay(Duration.ONE_SECOND).until(() => true)

      var checkStatsFile = snapshotSourceTableStatsService.checkSnapshotSourceTableStatsJsonFile(DEFAULT_PROJECT, tableIdentity)
      assertFalse(checkStatsFile)

      val checkTable = snapshotSourceTableStatsService.checkHiveTable(DEFAULT_PROJECT, table, config, tableIdentity)
      assertTrue(checkTable)

      checkStatsFile = snapshotSourceTableStatsService.checkSnapshotSourceTableStatsJsonFile(DEFAULT_PROJECT, tableIdentity)
      assertTrue(checkStatsFile)

      try {
        deleteMarkFile()
        checkStatsFile = snapshotSourceTableStatsService.checkSnapshotSourceTableStatsJsonFile(DEFAULT_PROJECT, tableIdentity)
        assertFalse(checkStatsFile)
      } finally {
        writeMarkFile()
      }
    })
  }

  test("checkSnapshotSourceTableStatsJsonFile - overwrite mark file") {
    val tableName = "hive_table_types" + RandomUtil.randomUUIDStr().replace("-", "_")
    hiveTable(tableName, (config, catalog, table) => {
      val tableIdentity = table.qualifiedName.toLowerCase(Locale.ROOT)
      writeEmptyJsonFile(tableIdentity)
      writeMarkFile()
      await.pollDelay(Duration.ONE_SECOND).until(() => true)

      var checkStatsFile = snapshotSourceTableStatsService.checkSnapshotSourceTableStatsJsonFile(DEFAULT_PROJECT, tableIdentity)
      assertFalse(checkStatsFile)

      val checkTable = snapshotSourceTableStatsService.checkHiveTable(DEFAULT_PROJECT, table, config, tableIdentity)
      assertTrue(checkTable)

      checkStatsFile = snapshotSourceTableStatsService.checkSnapshotSourceTableStatsJsonFile(DEFAULT_PROJECT, tableIdentity)
      assertTrue(checkStatsFile)

      await.pollDelay(Duration.ONE_SECOND).until(() => true)
      writeMarkFile()
      checkStatsFile = snapshotSourceTableStatsService.checkSnapshotSourceTableStatsJsonFile(DEFAULT_PROJECT, tableIdentity)
      assertFalse(checkStatsFile)
    })
  }
}
