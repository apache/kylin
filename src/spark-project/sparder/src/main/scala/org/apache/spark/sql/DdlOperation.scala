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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DDLDesc.DDLType
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIdentifier}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.{CommandExecutionMode, CommandResultExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.types.StructField
import java.lang.{String => JString}
import java.util.{List => JList}

import org.apache.spark.sql.delta.DeltaTableUtils

import scala.collection.JavaConverters._


object DdlOperation extends Logging {

  def executeSQL(sqlText: String): DDLDesc = {
    val logicalPlan: LogicalPlan = SparderEnv.getSparkSession.sessionState.sqlParser.parsePlan(sqlText)
    val queryExecution: QueryExecution = SparderEnv.getSparkSession.sessionState.executePlan(logicalPlan,
      CommandExecutionMode.SKIP)
    val currentDatabase: String = SparderEnv.getSparkSession.catalog.currentDatabase
    stripRootCommandResult(queryExecution.executedPlan) match {
      case ExecutedCommandExec(create: CreateTableCommand) =>
        val tableIdentifier: TableIdentifier = create.table.identifier
        if (create.table.tableType == CatalogTableType.MANAGED) {
          throw new RuntimeException(s"Table ${tableIdentifier} is managed table.Please modify to external table")
        }
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, tableIdentifier.database.getOrElse(currentDatabase), tableIdentifier.table, DDLType.CREATE_TABLE)
      case ExecutedCommandExec(view: CreateViewCommand) =>
        val viewIdentifier: TableIdentifier = view.name
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, viewIdentifier.database.getOrElse(currentDatabase), viewIdentifier.table, DDLType.CREATE_VIEW)
      case ExecutedCommandExec(drop: DropTableCommand) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, drop.tableName.database.getOrElse(currentDatabase),
          drop.tableName.table,
          DDLType.DROP_TABLE)
      case ExecutedCommandExec(db: CreateDatabaseCommand) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, db.databaseName,
          null,
          DDLType.CREATE_DATABASE)
      case ExecutedCommandExec(db: DropDatabaseCommand) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, db.databaseName,
          null,
          DDLType.DROP_DATABASE)
      case ExecutedCommandExec(addPartition: AlterTableAddPartitionCommand) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, addPartition.tableName.database.getOrElse(currentDatabase),
          addPartition.tableName.table, DDLType.ADD_PARTITION)
      case _ =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, null, null, DDLType.NONE)
    }
  }

  implicit class RichStructField(structField: StructField) {
    def toViewDDL: String = {
      val comment: Option[JString] = structField.getComment()
        .map(escapeSingleQuotedString)
        .map(" COMMENT '" + _ + "'")
      s"${quoteIdentifier(structField.name)}${comment.getOrElse("")}"
    }
  }

  def msck(database: JString, table: JString): JList[String] = {
    val before: Seq[Row] = calculatePartition(database, table)
    val tableIdentifier: JString = database + "." + table
    logInfo(s"Before ${tableIdentifier} msck partition number is ${before.size}")
    SparderEnv.getSparkSession.sql(s"msck repair table ${tableIdentifier}")
    val after: Seq[Row] = calculatePartition(database, table)
    logInfo(s"After ${tableIdentifier} msck partition number is ${after.size}")
    val diff: Seq[Row] = after.diff(before)
    diff.map(row => row.getString(0)).asJava
  }

  def hasPartition(database: String, table: String): Boolean = {
    val catalog: SessionCatalog = SparderEnv.getSparkSession.sessionState.catalog
    val catalogTable: CatalogTable = catalog.getTableMetadata(TableIdentifier(table, Some(database)))
    if (catalogTable.tableType != CatalogTableType.VIEW) {
      catalogTable.partitionColumnNames.nonEmpty && catalogTable.storage.locationUri.nonEmpty
    } else {
      false
    }
  }

  def getTableDesc(database: String, table: String): String = {
    var sql = s"SHOW CREATE TABLE $database.$table"
    var tableMetadata = SparderEnv.getSparkSession.sessionState.catalog
      .getTableRawMetadata(TableIdentifier(table, Some(database)))
    if (DeltaTableUtils.isDeltaTable(tableMetadata)) {
      return  new ShowCreateTableCommand(TableIdentifier(table, Some(database)), Seq.empty).
        run(SparderEnv.getSparkSession).toList.take(1).head.getString(0);
    }
    sql = if (DDLUtils.isHiveTable(tableMetadata)) sql + " AS SERDE" else sql
    val logicalPlan = SparderEnv.getSparkSession.sessionState.sqlParser.parsePlan(sql)
    val queryExecution: QueryExecution = SparderEnv.getSparkSession.sessionState.executePlan(logicalPlan,
      CommandExecutionMode.SKIP)
    stripRootCommandResult(queryExecution.executedPlan) match {
      case ExecutedCommandExec(show: ShowCreateTableCommand) => collectDDL(show.table, sql)
      case ExecutedCommandExec(show: ShowCreateTableAsSerdeCommand) => collectDDL(show.table, sql)
    }
  }


  def collectDDL(tableIdentifier: TableIdentifier, sql: String): String = {
    val catalog: SessionCatalog = SparderEnv.getSparkSession.sessionState.catalog
    val metadata: CatalogTable = catalog.getTableMetadata(tableIdentifier)
    metadata.tableType match {
      case CatalogTableType.VIEW =>
        val builder = new StringBuilder
        builder ++= s"CREATE VIEW ${tableIdentifier.quotedString}"
        if (metadata.schema.nonEmpty) {
          builder ++= metadata.schema.map(_.toViewDDL).mkString("(", ", ", ")")
        }
        builder ++= metadata.viewText.mkString(" AS\n", "", "\n")
        builder.toString()
      case CatalogTableType.MANAGED => ""
      case CatalogTableType.EXTERNAL => SparderEnv.getSparkSession.sql(sql).takeAsList(1).get(0).getString(0)
    }
  }


  def calculatePartition(database: String, table: String): Seq[Row] = {
    val logicalPlan: LogicalPlan = SparderEnv.getSparkSession.sessionState.sqlParser.parsePlan(s"show partitions ${database}.${table}")
    val queryExecution: QueryExecution = SparderEnv.getSparkSession.sessionState.executePlan(logicalPlan,
      CommandExecutionMode.SKIP)
    stripRootCommandResult(queryExecution.executedPlan) match {
      case ExecutedCommandExec(showPartitions: ShowPartitionsCommand) =>
        val rows: Seq[Row] = showPartitions.run(SparderEnv.getSparkSession)
        rows
    }
  }

  private def stripRootCommandResult(executedPlan: SparkPlan): SparkPlan = executedPlan match {
    case CommandResultExec(_, plan, _) => plan
    case other => other
  }
}
