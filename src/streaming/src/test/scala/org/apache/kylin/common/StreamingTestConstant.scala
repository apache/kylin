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
package org.apache.kylin.common

import org.apache.kylin.common.util.TempMetadataBuilder

object StreamingTestConstant {
  val KAP_SSB_STREAMING_TABLE = TempMetadataBuilder.TEMP_TEST_METADATA + "/data/SSB.P_LINEORDER.csv"
  val KAFKA_QPS = 500

  val High_KAFKA_QPS = 10000

  val CHECKPOINT_LOCATION = "hdfs://localhost:8020/spark/checkpoint/ssb"
  val INTERVAL = 1000
  val PROJECT = "streaming_test"
  val DATAFLOW_ID = "511a9163-7888-4a60-aa24-ae735937cc87"
  val SQL_FOLDER = "../spark-project/spark-it/src/test/resources/ssb"

  val COUNTDISTINCT_DATAFLOWID = "511a9163-7888-4a60-aa24-ae735937cc88"
  val COUNTDISTINCT_SQL_FOLDER = "../spark-project/spark-it/src/test/resources/count_distinct"
  val COUNTDISTINCT_CHECKPOINT_LOCATION = "hdfs://localhost:8020/spark/checkpoint/count_distinct"

  val MERGE_DATAFLOWID = "511a9163-7888-4a60-aa24-ae735937cc89"
  val MERGE_CHECKPOINT_LOCATION = "hdfs://localhost:8020/spark/checkpoint/merge"

  val BATCH_ROUNDS = 5
  val KAP_SSB_STREAMING_JSON_FILE = TempMetadataBuilder.TEMP_TEST_METADATA + "/data/SSB.P_LINEORDER.json"

}
