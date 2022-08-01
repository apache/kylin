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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfter

import java.sql.DriverManager
import java.util.Properties

class ShardJDBCSuite extends QueryTest
  with BeforeAndAfter
  with SharedSparkSession {

  val url0 = "jdbc:h2:mem:testdb0"
  var conn0: java.sql.Connection = _
  val url1 = "jdbc:h2:mem:testdb1"
  var conn1: java.sql.Connection = _

  private def createDB(url: String): java.sql.Connection = {
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    val conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      s"create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement(s"insert into test.people values ('fred', 1)").executeUpdate()
    conn.prepareStatement(s"insert into test.people values ('mary', 2)").executeUpdate()
    conn.prepareStatement(
      s"insert into test.people values ('bob', 3)").executeUpdate()
    conn.commit()
    conn
  }

  before {
    conn0 = createDB(url0)
    conn1 = createDB(url1)
    val sharding = s"${ShardOptions.SHARD_URLS} '${ShardOptions.buildSharding(url1, url0)}'"
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW foobar
         |USING org.apache.spark.sql.execution.datasources.jdbc.ShardJdbcRelationProvider
         |OPTIONS (url '$url0', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass', $sharding)
       """.stripMargin.replaceAll("\n", " "))
  }

  after {
    conn0.close()
    conn1.close()
  }

  test("simple scan") {
    assert(sql("SELECT * FROM foobar").collect().length === 6)
  }
}
