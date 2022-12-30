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
package org.apache.kylin.rest.ddl

import java.security.PrivilegedExceptionAction

import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `map AsScala`}
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kylin.common.msg.MsgPicker
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.source.NSparkMetadataExplorer
import org.apache.kylin.metadata.model.NTableMetadataManager
import org.apache.kylin.metadata.view.LogicalViewManager
import org.apache.kylin.rest.security.KerberosLoginManager
import org.slf4j.LoggerFactory

import org.apache.spark.ddl.{DDLCheck, DDLCheckContext, DDLConstant}
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.{CommandExecutionMode, CommandResultExec, SparkPlan}
import org.apache.spark.sql.execution.command._

class ViewCheck extends DDLCheck {
  private val LOGGER = LoggerFactory.getLogger(classOf[ViewCheck])
  private val PREFIX = "KE_"
  private val SOURCE = new NSparkMetadataExplorer
  private val LOGICAL_VIEW_TYPE = "GlobalTempView"

  override def description(project: String, pageType: String): Array[String] = {
    val config = KylinConfig.getInstanceFromEnv
    val cnDescription: StringBuilder = new StringBuilder
    val enDescription: StringBuilder = new StringBuilder
    val databasesHasAccess: StringBuilder = new StringBuilder
    val syntaxSupport: StringBuilder = new StringBuilder
    if ("hive".equalsIgnoreCase(pageType)) {
      databasesHasAccess.append(listAllDatabasesHasAccess(project))
      syntaxSupport.append("`create view`,`alter view`,`drop view`,`show create table`")
      cnDescription.append("Hive View 名称需要以 `KE_` 开头\n")
      enDescription.append("Hive View name should start with `KE_`\n")
      cnDescription
        .append(s"仅支持 ${syntaxSupport} 语法\n")
      enDescription
        .append(s"Only supports ${syntaxSupport} syntax\n")
      cnDescription.append(s"仅支持创建 Hive View 在如下数据库: ${databasesHasAccess}\n")
      enDescription.append(s"Only supports creating Hive Views in ${databasesHasAccess}\n")
    } else {
      cnDescription.append(s"创建不要加 database 名称，系统自动创建到 ${config.getDDLLogicalViewDB} 库中，"
        + s"删除要加 ${config.getDDLLogicalViewDB} 库名称 \n")
      enDescription.append(s"Creating does not require adding database, it is automatically created in"
        + s" ${config.getDDLLogicalViewDB} , deleting should add ${config.getDDLLogicalViewDB} database\n")
      syntaxSupport.append(" `create logical view`, `drop logical view` ")
      cnDescription
        .append(s"仅支持 ${syntaxSupport} 语法\n")
      enDescription
        .append(s"Only supports ${syntaxSupport} syntax\n")
    }


    Array(
      enDescription.toString(),
      cnDescription.toString())
  }

  override def priority: Int = DDLConstant.VIEW_RULE_PRIORITY

  override def check(context: DDLCheckContext): Unit = {
    LOGGER.info("start checking DDL view name")
    val sql = context.getSql
    val project = context.getProject
    val spark = SparderEnv.getSparkSession
    val config = KylinConfig.getInstanceFromEnv
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
        if (view.viewType != null && LOGICAL_VIEW_TYPE.equalsIgnoreCase(view.viewType.toString())) {
          val viewManager = LogicalViewManager.getInstance(config)
          val originTable = viewManager.get(view.name.table)
          if (view.replace) {
            context.setCommandType(DDLConstant.REPLACE_LOGICAL_VIEW)
            if (originTable == null) {
              throwException("View name is not found.")
            }
            if (!originTable.getCreatedProject.equals(context.getProject)) {
              throwException(s"View can only modified in Project ${originTable.getCreatedProject}")
            }
          } else {
            if (originTable != null) {
              throwException(MsgPicker.getMsg.getDDLViewNameDuplicateError)
            }
            context.setCommandType(DDLConstant.CREATE_LOGICAL_VIEW)
          }
          context.setLogicalViewName(view.name.table)
        } else {
          checkHiveTableName(view.name, context)
          checkHiveDatabaseAccess(view.name, project, context)
        }
      case ExecutedCommandExec(view: ShowCreateTableCommand) =>
        checkHiveTableName(view.table, context)
        checkHiveDatabaseAccess(view.table, project, context)
      case ExecutedCommandExec(table: DropTableCommand) =>
        if (!table.isView) {
          throwException(MsgPicker.getMsg.getDDLDropError)
        }
        val tableIdentifier = table.tableName
        if (config.isDDLLogicalViewEnabled && tableIdentifier.database.isDefined
          && config.getDDLLogicalViewDB.equalsIgnoreCase(tableIdentifier.database.get)) {
          context.setCommandType(DDLConstant.DROP_LOGICAL_VIEW)
          context.setLogicalViewName(tableIdentifier.table)
          checkLogicalViewNotUsed(tableIdentifier, context.getProject)
        } else {
          checkHiveTableName(table.tableName, context)
          checkHiveDatabaseAccess(table.tableName, project, context)
        }
      case ExecutedCommandExec(table: AlterViewAsCommand) =>
        checkHiveTableName(table.name, context)
        checkHiveDatabaseAccess(table.name, project, context)
      case _ => throwException(MsgPicker.getMsg.getDDLUnSupported)
    }
    if (context.isLogicalViewCommand && !config.isDDLLogicalViewEnabled) {
      throwException("Logical View operation is not supported, please turn on config.")
    }
    if (context.isHiveCommand && !config.isDDLHiveEnabled) {
      throwException("Hive operation is not supported, please turn on config.")
    }
    checkCommandRestrict(context)
  }

  private def checkHiveTableName(identifier: TableIdentifier, context: DDLCheckContext): Unit = {
    if (!identifier.table.toUpperCase().startsWith(PREFIX)) {
      throwException(MsgPicker.getMsg.getDDLViewNameError)
    }
  }

  def checkHiveDatabaseAccess(identifier: TableIdentifier, project: String, context: DDLCheckContext): Unit = {
    if (!context.isKerberosEnv && !KylinConfig.getInstanceFromEnv.isUTEnv) {
      return
    }
    if (identifier.database.isEmpty) {
      throwException("Missing Databases name in sql.")
    }
    val database = identifier.database.get
    if (database.equalsIgnoreCase(KylinConfig.getInstanceFromEnv.getDDLLogicalViewDB)) {
      throwException("Shouldn't use logical view database.")
    }
    val ugi = KerberosLoginManager.getInstance.getProjectUGI(project)
    val hasDatabaseAccess = ugi.doAs(new PrivilegedExceptionAction[Boolean]() {
      override def run(): Boolean = {
        SOURCE.checkDatabaseHadoopAccessFast(database)
      }
    })
    if (!hasDatabaseAccess) {
      throwException(MsgPicker.getMsg.getDDLDatabaseAccessnDenied)
    }
  }

  def checkLogicalViewNotUsed(tableIdentity: TableIdentifier, project: String): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val db = config.getDDLLogicalViewDB
    val viewName = tableIdentity.table
    val tableManager = NTableMetadataManager.getInstance(config, project)
    tableManager.listTablesGroupBySchema.
      filter(dbInfo => dbInfo._1.equalsIgnoreCase(db))
      .foreach(dbInfo => {
        val isExist = dbInfo._2.exists(loadTable => {
          loadTable.getName.equalsIgnoreCase(viewName)
        })
        if (isExist) {
          throwException(MsgPicker.getMsg.getDDLLogicalViewHasUsed(viewName, project))
        }
      })
    val viewManager = LogicalViewManager.getInstance(config)
    val originTable = viewManager.get(viewName)
    if (originTable != null && !originTable.getCreatedProject.equalsIgnoreCase(project)) {
      throwException(s"View can only modified in Project ${originTable.getCreatedProject}")
    }
  }

  def listAllDatabasesHasAccess(project: String): String = {
    val shouldCheckKerberosAccess = UserGroupInformation.isSecurityEnabled
    val ugi = KerberosLoginManager.getInstance.getProjectUGI(project)
    val databasesHasAccess = ugi.doAs(new PrivilegedExceptionAction[List[String]]() {
      override def run(): List[String] = {
        val databases = SOURCE.listDatabases()
        val databasesHasAccess = ListBuffer[String]()
        databases.forEach(db => {
          if (!shouldCheckKerberosAccess || SOURCE.checkDatabaseHadoopAccessFast(db)) {
            databasesHasAccess.append(db)
          }
        })
        databasesHasAccess.toList
      }
    })
    databasesHasAccess.mkString(",")
  }

  def checkCommandRestrict(context: DDLCheckContext): Unit = {
    val restrict = context.getRestrict
    val commandType = context.getCommandType
    if (StringUtils.isBlank(restrict)) {
      return
    }
    if (restrict.equalsIgnoreCase(DDLConstant.LOGICAL_VIEW) && !(commandType.equalsIgnoreCase(DDLConstant.CREATE_LOGICAL_VIEW)
      || commandType.equalsIgnoreCase(DDLConstant.DROP_LOGICAL_VIEW))) {
      throwException(
        MsgPicker.getMsg.getDDLRestrictError("`create logical view`, `drop logical view`"))
    } else if (restrict.equalsIgnoreCase(DDLConstant.REPLACE_LOGICAL_VIEW) && !restrict.equalsIgnoreCase(commandType)) {
      throwException(
        MsgPicker.getMsg.getDDLRestrictError("`replace logical view`"))
    } else if (restrict.equalsIgnoreCase(DDLConstant.HIVE_VIEW) && !restrict.equalsIgnoreCase(commandType)) {
      throwException(
        MsgPicker.getMsg.getDDLRestrictError("`create view`,`alter view`,`drop view`,`show create table`"))
    } else if (!(restrict.equalsIgnoreCase(DDLConstant.HIVE_VIEW) || restrict.equalsIgnoreCase(DDLConstant.LOGICAL_VIEW) ||
      restrict.equalsIgnoreCase(DDLConstant.REPLACE_LOGICAL_VIEW))) {
      throwException(s"illegal restrict: ${restrict}")
    }
  }

  private def stripRootCommandResult(executedPlan: SparkPlan) = executedPlan match {
    case CommandResultExec(_, plan, _) => plan
    case other => other
  }
}
