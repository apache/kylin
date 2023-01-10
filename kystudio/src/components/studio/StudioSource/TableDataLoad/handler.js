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
export function _getPartitionInfo (project, table, partitionColumn, format) {
  return {
    project: project.name,
    table: `${table.database}.${table.name}`,
    column: partitionColumn,
    partition_column_format: format
  }
}

export function _getRefreshFullLoadInfo (project, table) {
  return {
    projectName: project.name,
    tableFullName: `${table.database}.${table.name}`,
    startTime: '0',
    endTime: '9223372036854775807'
  }
}

export function _getFullLoadInfo (project, table) {
  return {
    projectName: project.name,
    tableFullName: `${table.database}.${table.name}`,
    startTime: String(0),
    endTime: '9223372036854775807',
    affectedStart: '',
    affectedEnd: ''
  }
}
