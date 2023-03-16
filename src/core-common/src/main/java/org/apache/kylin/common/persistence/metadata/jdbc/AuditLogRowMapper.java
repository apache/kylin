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
package org.apache.kylin.common.persistence.metadata.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.persistence.AuditLog;
import org.springframework.jdbc.core.RowMapper;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.SneakyThrows;
import lombok.val;

public class AuditLogRowMapper implements RowMapper<AuditLog> {
    @SneakyThrows
    @Override
    public AuditLog mapRow(ResultSet rs, int rowNum) throws SQLException {
        val id = rs.getLong(1);
        val resPath = rs.getString(2);
        val content = CompressionUtils.decompress(rs.getBytes(3));
        Long ts = rs.getLong(4);
        if (rs.wasNull()) {
            ts = null;
        }
        Long mvcc = rs.getLong(5);
        if (rs.wasNull()) {
            mvcc = null;
        }
        val unitId = rs.getString(6);
        val operator = rs.getString(7);
        val instance = rs.getString(8);

        return new AuditLog(id, resPath, content == null ? null : ByteSource.wrap(content), ts, mvcc, unitId, operator,
                instance);
    }
}
