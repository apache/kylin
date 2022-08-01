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
package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation

class ShardJdbcRelationProvider extends JdbcRelationProvider {
  override def shortName(): String = "shard_jdbc"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val relation = super.createRelation(sqlContext, parameters)
    assert(relation.isInstanceOf[JDBCRelation])
    val jdbcRelation = relation.asInstanceOf[JDBCRelation]
    val shards = ShardOptions.create(jdbcRelation.jdbcOptions)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val parts = if (shards.shards.length == 1) {
      JDBCRelation.columnPartition(jdbcRelation.schema, resolver, timeZoneId, jdbcRelation.jdbcOptions)
    } else {
      ShardJDBCUtil.shardPartition(shards)
    }
    jdbcRelation.copy(parts = parts)(jdbcRelation.sparkSession)
  }
}


