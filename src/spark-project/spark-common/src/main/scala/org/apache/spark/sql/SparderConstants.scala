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
package org.apache.spark.sql.util

object SparderConstants {
  val KYLIN_SCAN_GTINFO_BYTES = "io.kylin.storage.parquet.scan.gtinfo"
  val KYLIN_SCAN_REQUEST_BYTES = "io.kylin.storage.parquet.scan.gtscanrequest"
  val SPARK_PROJECT_SCHEMA = "org.apache.spark.sql.sparder.row.project"
  val PAGE_FILTER_PUSH_DOWN = "page_filter_push_down"
  val BINARY_FILTER_PUSH_DOWN = "binary_filter_push_down"
  val LATE_DECODE_COLUMN = "late_decode_column"
  val CUBOID_ID = "cuboid_id"
  val TABLE_ALIAS = "table_alias"
  val STORAGE_TYPE = "storage_type"
  val COLUMN_PREFIX = "col_"
  val DICT = "dict"
  val DICT_PATCH = "dict_patch"
  val DIAGNOSIS_WRITER_TYPE = "diagnosisWriterType"
  val SEGMENT_ID = "segmentId"
  val COLUMN_NAMES_SEPARATOR = "SEPARATOR"
  val COLUMN_NAME_SEPARATOR = "_0_LINE_0_"
  val DERIVE_TABLE = "DERIVE"
  val DATASCHMEA_TABLE_ORD = 0
  val DATASCHMEA_COL_ORD = 1
  val DATASCHMEA_DATATYPE_ORD = 2
  val DATASTYPESCHMEA_COL_ORD = 0
  val DATASTYPESCHMEA_DATA_ORD = 1
  val PARQUET_FILE_FILE_TYPE = "file_type"
  val PARQUET_FILE_RAW_TYPE = "raw"
  val KYLIN_CONF = "kylin_conf"
  val PARQUET_FILE_CUBE_TYPE = "cube"
  val DATE_DICT = "date_dict"
}
