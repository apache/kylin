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

package io.kyligence.kap.clickhouse.job;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

@Slf4j
@Getter
public class ClickHouse implements Closeable {
    public static final int JDBC_PREFIX = "jdbc:clickhouse://".length();
    public static final String PASSWORD = "password";
    public static final String USER = "user";
    public static final String SOCKET_TIMEOUT = "socket_timeout";
    public static final String KEEP_ALIVE_TIMEOUT = "keepAliveTimeout";
    public static final String CLIENT_NAME = "client_name";

    private final String shardName;
    private Connection connection;
    private final String preprocessedUrl;
    private final Properties properties;


    public ClickHouse(String jdbcUrl) {
        String user = null;
        String password = null;
        preprocessedUrl = jdbcUrl.contains("?") ? jdbcUrl.split("\\?")[0] : jdbcUrl;
        this.shardName = preprocessedUrl.trim().substring(JDBC_PREFIX);
        val param = extractParam(jdbcUrl);
        if (param.containsKey(USER)) {
            user = param.get(USER);
        }
        if (param.containsKey(PASSWORD)) {
            password = param.get(PASSWORD);
        }
        properties = new Properties();
        properties.setProperty(SOCKET_TIMEOUT, param.getOrDefault(SOCKET_TIMEOUT, "600000"));
        properties.setProperty(KEEP_ALIVE_TIMEOUT, param.getOrDefault(KEEP_ALIVE_TIMEOUT, "600000"));
        if (user != null) {
            properties.setProperty(USER, user);
        }
        if (password != null) {
            properties.setProperty(PASSWORD, password);
        }
    }

    public void connect() throws SQLException {
        if (this.connection == null) {
            this.connection = DriverManager.getConnection(preprocessedUrl, properties);
        }
    }

    public static Map<String, String> extractParam(String jdbcUrl) {
        String query = jdbcUrl.contains("?") ? jdbcUrl.split("\\?")[1] : "";
        if (StringUtils.isBlank(query)) return Collections.emptyMap();
        Map<String, String> params = new HashMap<>();
        for (String s : query.split("&")) {
            if (StringUtils.isBlank(s)) continue;
            String[] pair = s.split("=");
            params.put(pair[0], pair[1]);
        }
        return params;
    }

    public static String buildUrl(String ip, int port, Map<String, String> param) {
        return buildUrl(ip + ":" + port, param);
    }

    public static String buildUrl(String address, Map<String, String> param) {
        StringBuilder base = new StringBuilder("jdbc:clickhouse://" + address);
        if (!param.isEmpty()) {
            base.append('?');
            List<String> paramList = new ArrayList<>();
            param.forEach((name, value) -> paramList.add(name + "=" + value));
            base.append(String.join("&", paramList));
        }
        return base.toString();
    }

    private void logSql(String sql) {
        log.info("Execute SQL '{}' on [{}]", sql, shardName);
    }

    public boolean apply(String sql) throws SQLException {
        connect();
        logSql(sql);

        try (Statement stmt = connection.createStatement()) {
            return stmt.execute(sql);
        }
    }


    public <T> List<T> query(String sql, Function<ResultSet, T> resultParser) throws SQLException {
        connect();
        logSql(sql);
        val stmt = connection.createStatement();
        val result = new ArrayList<T>();
        try (ResultSet resultSet = stmt.executeQuery(sql)) {
            while (resultSet.next()) {
                result.add(resultParser.apply(resultSet));
            }
        }
        return result;
    }

    public List<Date> queryPartition(String sql, String pattern) throws SQLException {
        connect();
        logSql(sql);
        val stmt = connection.createStatement();
        val result = new ArrayList<Date>();
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern, Locale.ROOT);
        try (ResultSet resultSet = stmt.executeQuery(sql)) {
            val type = resultSet.getMetaData().getColumnType(1);
            while (resultSet.next()) {
                if (Types.DATE == type) {
                    result.add(resultSet.getDate(1));
                } else if (Types.VARCHAR == type) {
                    try {
                        Date date = new Date(dateFormat.parse(resultSet.getString(1)).getTime());
                        result.add(date);
                    } catch (ParseException e) {
                        ExceptionUtils.rethrow(e);
                    }
                } else {
                    try {
                        Date date = new Date(dateFormat.parse(Objects.toString(resultSet.getObject(1))).getTime());
                        result.add(date);
                    } catch (ParseException e) {
                        ExceptionUtils.rethrow(e);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException sqlException) {
            log.error("{} close failed", shardName);
        }
    }
}
