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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.sql.SQLException

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.read.sqlpushdown.SupportsSQL
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ShardJDBCTableCatalog extends JDBCTableCatalog with SupportsSQL {
  private val DEFAULT_CLICKHOUSE_URL = "jdbc:clickhouse://localhost:9000"

  private var options: JDBCOptions = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // when url is empty, use default url
    val urlKey = "url"
    var modifiedOptions = options.asCaseSensitiveMap().asScala.toMap
    if (!options.containsKey(urlKey)) {
      modifiedOptions += (urlKey -> DEFAULT_CLICKHOUSE_URL)
    }
    val newOptions = new CaseInsensitiveStringMap(modifiedOptions.asJava)
    super.initialize(name, newOptions)

    val optionsField = classOf[JDBCTableCatalog].getDeclaredField("options")
    optionsField.setAccessible(true)
    this.options = optionsField.get(this).asInstanceOf[JDBCOptions]
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    val optionsWithTableName = new JDBCOptions(
      options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
    try {
      val schema = resolveTable(ident).getOrElse(JDBCRDD.resolveTable(optionsWithTableName))
      new ShardJDBCTable(ident, schema, optionsWithTableName)
    } catch {
      case _: SQLException => throw QueryCompilationErrors.noSuchTableError(ident)
    }
  }

  protected def resolveTable(ident: Identifier): Option[StructType] = {
    None
  }

  private lazy val jdbcTableCatalog = classOf[JDBCTableCatalog]

  private lazy val methodGetTableName = {
    val method = jdbcTableCatalog.getDeclaredMethod("getTableName", classOf[Identifier])
    method.setAccessible(true)
    method
  }

  private lazy val methodCheckNamespace = {
    val method = jdbcTableCatalog.getDeclaredMethod("checkNamespace", classOf[Array[String]])
    method.setAccessible(true)
    method
  }

  private def checkNamespace(namespace: Array[String]): Unit = {
    methodCheckNamespace.invoke(this, namespace)
  }

  protected def getTableName(ident: Identifier): String = {
    methodGetTableName.invoke(this, ident).asInstanceOf[String]
  }
}
