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
package org.apache.kylin.common.persistence.metadata;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import lombok.val;

public class EpochRowMapper implements RowMapper<Epoch> {
    @Override
    public Epoch mapRow(ResultSet rs, int rowNum) throws SQLException {
        val epochId = rs.getLong(1);
        val epochTarget = rs.getString(2);
        val currentEpochOwner = rs.getString(3);
        val lastEpochRenewTime = rs.getLong(4);
        val serverMode = rs.getString(5);
        val maintenanceModeReason = rs.getString(6);
        val mvcc = rs.getLong(7);

        return new Epoch(epochId, epochTarget, currentEpochOwner, lastEpochRenewTime, serverMode, maintenanceModeReason,
                mvcc);
    }
}
