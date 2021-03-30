/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark

import java.util.Locale

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkSqlOnLivyBatch extends Logging{

  def main(args: Array[String]) {

    if (args.length != 1) {
      log.info("Usage: SparkSqlOnLivyBatch <sqlstring>")
      System.exit(1)
    }

    val sql : String = args(0)
    log.info(String.format(Locale.ROOT, "Sql-Info : %s", sql))

    val spark = SparkSession.builder().enableHiveSupport().appName("kylin-sql-livy").getOrCreate()

    val sqlStrings = sql.split(";")

    for (sqlString <- sqlStrings) {
      var item = sqlString.trim()
      if (item.length > 0) {
        if (StringUtils.endsWith(item, "\\")) {
          item = StringUtils.chop(item)
        }
        spark.sql(item)
      }
    }
  }
}
