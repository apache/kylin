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

import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

import scala.collection.JavaConverters._

class BooleanTypeTest extends SparderBaseFunSuite with SharedSparkSession {

  test("Boolean") {
    val schema = StructType(Seq(StructField("name", BooleanType, true)))
    val rows = List(Row(false), Row(null)).asJava
    val frame = spark.createDataFrame(rows, schema)
    frame.select("name").show(10)
  }

}
