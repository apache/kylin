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

package org.apache.kylin.job.metrics;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.kylin.common.KylinConfig;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;

import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Date;
import java.util.TimeZone;

public class JobMetricsTable extends SqlTable {

    public final SqlColumn<Long> id = column("id", JDBCType.BIGINT);
    public final SqlColumn<String> jobId = column("job_id", JDBCType.VARCHAR);
    public final SqlColumn<String> submitter = column("submitter", JDBCType.VARCHAR);
    public final SqlColumn<String> model = column("model", JDBCType.VARCHAR);
    public final SqlColumn<String> jobType = column("job_type", JDBCType.VARCHAR);
    public final SqlColumn<String> projectName = column("project_name", JDBCType.VARCHAR);
    public final SqlColumn<Long> duration = column("duration", JDBCType.BIGINT);
    public final SqlColumn<Long> modelSize = column("model_size", JDBCType.BIGINT);
    public final SqlColumn<Long> waitTime = column("wait_time", JDBCType.BIGINT);
    public final SqlColumn<Long> buildTime = column("build_time", JDBCType.BIGINT);
    public final SqlColumn<Long> buildFirstDayOfMonth = column("build_first_day_of_month", JDBCType.BIGINT);
    public final SqlColumn<Long> buildFirstDayOfWeek = column("build_first_day_of_week", JDBCType.BIGINT);
    public final SqlColumn<Long> buildDay = column("build_day", JDBCType.BIGINT);
    public final SqlColumn<Date> buildDate = column("build_date", JDBCType.DATE);

    public final SqlColumn<String> jobState = column("job_state", JDBCType.VARCHAR);

    public final SqlColumn<String> errorType = column("error_type", JDBCType.VARCHAR);

    public final SqlColumn<String> errorInfo = column("error_info", JDBCType.VARCHAR);

    public final SqlColumn<Double> perBytesTimeCost = column("per_bytes_time_cost", JDBCType.DOUBLE);

    public final SqlColumn<String> jobEngine = column("job_engine", JDBCType.VARCHAR);


    public JobMetricsTable(String tableName) {
        super(tableName);
    }

    public static class InstantHandler implements TypeHandler<Instant> {

        @Override
        public void setParameter(PreparedStatement ps, int i, Instant parameter, JdbcType jdbcType)
                throws SQLException {
            ps.setLong(i, parameter.toEpochMilli());
        }

        @Override
        public Instant getResult(ResultSet rs, String columnName) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(rs.getLong(columnName)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }

        @Override
        public Instant getResult(ResultSet rs, int columnIndex) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(rs.getLong(columnIndex)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }

        @Override
        public Instant getResult(CallableStatement cs, int columnIndex) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(cs.getLong(columnIndex)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }
    }
}
