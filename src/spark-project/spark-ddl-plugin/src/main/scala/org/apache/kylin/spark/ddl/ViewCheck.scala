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
package org.apache.kylin.spark.ddl

import java.security.PrivilegedExceptionAction

import scala.collection.mutable.ListBuffer

import org.apache.kylin.common.msg.MsgPicker
import org.apache.kylin.engine.spark.source.NSparkMetadataExplorer
import org.apache.kylin.rest.security.KerberosLoginManager
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.{CommandExecutionMode, CommandResultExec, SparkPlan}
import org.apache.spark.sql.execution.command._

class ViewCheck extends DDLCheck {
  private val log = LoggerFactory.getLogger(classOf[ViewCheck])
  private val PREFIX = "KE_"
  private val source = new NSparkMetadataExplorer

  override def description(project: String): Array[String] = {
    val databasesHasAccess = listAllDatabasesHasAccess(project)
    Array(
      "View name should start with `KE_`\n"
        + "Only support `create view`,`alter view`,`drop view`,`show create table` syntax\n"
        + s"Only supports creating views in ${databasesHasAccess}",
      "View 名称需要以`KE_`开头\n"
        + "仅支持 `create view`, `drop view`, `alter view`, `show create table` 语法\n"
        + s"仅支持在 ${databasesHasAccess} 上述 database 中创建 view")
  }

  override def check(context: DDLCheckContext): Unit = {
    log.info("start checking DDL view name")
    val sql = context.getSql
    val project = context.getProject
    val spark = SparderEnv.getSparkSession
    var plan: SparkPlan = null
    try {
      val logicalPlan = spark.sessionState.sqlParser.parsePlan(sql)
      plan = stripRootCommandResult(spark.sessionState.executePlan(
        logicalPlan, CommandExecutionMode.SKIP).executedPlan)
    } catch {
      case e: Exception => throwException(e.getMessage)
    }
    plan match {
      case ExecutedCommandExec(view: CreateViewCommand) =>
        checkTableName(view.name)
        checkAccess(view.name, project)
      case ExecutedCommandExec(view: ShowCreateTableCommand) =>
        checkTableName(view.table)
        checkAccess(view.table, project)
      case ExecutedCommandExec(table: DropTableCommand) =>
        checkTableName(table.tableName)
        checkAccess(table.tableName, project)
        if (!table.isView) {
          throwException(MsgPicker.getMsg.getDDLDropError)
        }
      case ExecutedCommandExec(table: AlterViewAsCommand) =>
        checkTableName(table.name)
        checkAccess(table.name, project)
      case _ => throwException(MsgPicker.getMsg.getDDLUnSupported)
    }
  }

  private def checkTableName(identifier: TableIdentifier): Unit = {
    if (!identifier.table.toUpperCase().startsWith(PREFIX)) {
      throwException(MsgPicker.getMsg.getDDLViewNameError)
    }
  }

  def checkAccess(identifier: TableIdentifier, project: String): Unit = {
    val database = identifier.database.get
    val ugi = KerberosLoginManager.getInstance.getProjectUGI(project)
    val hasDatabaseAccess = ugi.doAs(new PrivilegedExceptionAction[Boolean]() {
      override def run(): Boolean = {
        source.checkDatabaseHadoopAccessFast(database)
      }
    })
    if (!hasDatabaseAccess) {
      throwException(MsgPicker.getMsg.getDDLDatabaseAccessnDenied)
    }
  }

  def listAllDatabasesHasAccess(project: String): String = {
    val ugi = KerberosLoginManager.getInstance.getProjectUGI(project)
    val databasesHasAccess = ugi.doAs(new PrivilegedExceptionAction[List[String]]() {
      override def run(): List[String] = {
        val databases = source.listDatabases()
        val databasesHasAccess = ListBuffer[String]()
        databases.forEach(db => {
          if (source.checkDatabaseHadoopAccessFast(db)) {
            databasesHasAccess.append(db)
          }
        })
        databasesHasAccess.toList
      }
    })
    databasesHasAccess.mkString(",")
  }

  private def stripRootCommandResult(executedPlan: SparkPlan) = executedPlan match {
    case CommandResultExec(_, plan, _) => plan
    case other => other
  }
}
