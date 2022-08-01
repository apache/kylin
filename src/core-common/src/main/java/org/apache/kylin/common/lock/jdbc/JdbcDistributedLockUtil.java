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

package org.apache.kylin.common.lock.jdbc;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.Locale;
import java.util.Properties;

@Slf4j
public class JdbcDistributedLockUtil {
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final String CREATE_TABLE_TEMPLATE = "create.jdbc.distributed.lock.table";

    private JdbcDistributedLockUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static void createDistributedLockTableIfNotExist() throws Exception {
        String prefix = getGlobalDictLockTablePrefix();
        String lockTableName = prefix + "LOCK";
        DataSource dataSource = getDataSource();
        try (Connection connection = dataSource.getConnection()) {
            if (JdbcUtil.isTableExists(connection, lockTableName)) {
                return;
            }
        } catch (Exception e) {
            log.error("Fail to know if table {} exists", lockTableName, e);
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties((BasicDataSource) dataSource);
            String sql = String.format(Locale.ROOT, properties.getProperty(CREATE_TABLE_TEMPLATE), prefix, prefix);
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(
                    new InputStreamReader(new ByteArrayInputStream(sql.getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static String getGlobalDictLockTablePrefix() {
        StorageURL url = KylinConfig.getInstanceFromEnv().getJDBCDistributedLockURL();
        return StorageURL.replaceUrl(url);
    }

    public static DataSource getDataSource() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val url = config.getJDBCDistributedLockURL();
        val props = JdbcUtil.datasourceParameters(url);
        return JdbcDataSource.getDataSource(props);
    }
}
