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

import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions.{REPLICA_SPLIT_CHAR, SPLIT_CHAR}

case class ShardOptions(sharding: String) {
  val replicaShards: Array[Array[String]] = sharding
    .split(REPLICA_SPLIT_CHAR)
    .map(_.split("\\" + SPLIT_CHAR))

  //  val shards: Array[String] = sharding.split(ShardOptions.SPLIT_CHAR)
  val shards: Array[String] = replicaShards(0)

}

object ShardOptions {
  val SHARD_URLS = "shard_urls"
  val PUSHDOWN_AGGREGATE = "pushDownAggregate"
  val PUSHDOWN_LIMIT = "pushDownLimit"
  val PUSHDOWN_NUM_PARTITIONS = "numPartitions"
  val SPLIT_CHAR = "<url_split>"
  val REPLICA_SPLIT_CHAR = "<replica_split>"

  def create(options: JDBCOptions): ShardOptions = {
    ShardOptions(options.parameters.get(SHARD_URLS).getOrElse(options.url))
  }

  def buildSharding(urls: String*): String = {
    urls.mkString(SPLIT_CHAR.toString)
  }

  def buildReplicaSharding(urls: Array[Array[String]]): String = {
    urls.map(_.mkString(SPLIT_CHAR.toString)).mkString(REPLICA_SPLIT_CHAR)
  }
}