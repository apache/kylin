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

package org.apache.kylin.metadata.streaming;

import java.sql.JDBCType;

import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;

public class StreamingJobStatsTable extends SqlTable {

    public final SqlColumn<Long> id = column("id", JDBCType.BIGINT);
    public final SqlColumn<String> jobId = column("job_id", JDBCType.VARCHAR);
    public final SqlColumn<String> projectName = column("project_name", JDBCType.VARCHAR);
    public final SqlColumn<Long> batchRowNum = column("batch_row_num", JDBCType.BIGINT);
    public final SqlColumn<Double> rowsPerSecond = column("rows_per_second", JDBCType.DOUBLE);
    public final SqlColumn<Long> processingTime = column("processing_time", JDBCType.BIGINT);
    public final SqlColumn<Long> minDataLatency = column("min_data_latency", JDBCType.BIGINT);
    public final SqlColumn<Long> maxDataLatency = column("max_data_latency", JDBCType.BIGINT);
    public final SqlColumn<Long> createTime = column("create_time", JDBCType.BIGINT);

    public StreamingJobStatsTable(String tableName) {
        super(tableName);
    }

}
