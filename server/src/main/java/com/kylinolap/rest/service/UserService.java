/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.security.provisioning.JdbcUserDetailsManager;

import com.kylinolap.rest.request.MetricsRequest;
import com.kylinolap.rest.response.MetricsResponse;

/**
 * @author xduo
 * 
 */
public class UserService extends JdbcUserDetailsManager {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    public void hit(final String username) {
        jdbcTemplate.update("insert into user_hits(username,hit_time) values(?,?);", new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setString(1, username);
                ps.setTimestamp(2, new java.sql.Timestamp(new Date().getTime()));
            }
        });
    }

    public List<String> getUserAuthorities() {
        return jdbcTemplate.queryForList("select distinct authority from authorities", new String[] {}, String.class);
    }

    public MetricsResponse calculateMetrics(MetricsRequest request) {
        MetricsResponse metrics = new MetricsResponse();
        Date startTime = (null == request.getStartTime()) ? new Date(-1) : request.getStartTime();
        Date endTime = (null == request.getEndTime()) ? new Date() : request.getEndTime();
        String userCountSql = "select count(distinct username) as count from user_hits where hit_time > ? and hit_time < ?";

        int userCount = (Integer) jdbcTemplate.queryForObject(userCountSql, new Object[] { startTime, endTime }, Integer.class);

        metrics.increase("userCount", (float) userCount);

        return metrics;
    }
}
