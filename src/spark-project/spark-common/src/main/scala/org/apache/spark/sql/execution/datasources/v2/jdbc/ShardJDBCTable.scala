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

import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ShardJDBCTable(ident: Identifier, schema: StructType, jdbcOptions: JDBCOptions)
  extends JDBCTable(ident, schema, jdbcOptions) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ShardJDBCScanBuilder = {
    val jdbcScanBuilder = super.newScanBuilder(options)
    new ShardJDBCScanBuilder(jdbcScanBuilder.session, schema, jdbcScanBuilder.jdbcOptions)
  }
}
