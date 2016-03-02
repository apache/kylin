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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Hive meta API client for Kylin
 * @author shaoshi
 *
 */
public class HiveClient {

    protected HiveConf hiveConf = null;
    protected Driver driver = null;
    protected HiveMetaStoreClient metaStoreClient = null;

    public HiveClient() {
        hiveConf = new HiveConf(HiveClient.class);
    }

    public HiveClient(Map<String, String> configMap) {
        this();
        appendConfiguration(configMap);
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    /**
     * Get the hive ql driver to execute ddl or dml
     * @return
     */
    private Driver getDriver() {
        if (driver == null) {
            driver = new Driver(hiveConf);
            SessionState.start(new CliSessionState(hiveConf));
        }

        return driver;
    }

    /**
     * Append or overwrite the default hive client configuration; You need call this before invoke #executeHQL;
     * @param configMap
     */
    public void appendConfiguration(Map<String, String> configMap) {
        if (configMap != null && configMap.size() > 0) {
            for (Entry<String, String> e : configMap.entrySet()) {
                hiveConf.set(e.getKey(), e.getValue());
            }
        }
    }

    /**
     * 
     * @param hql
     * @throws CommandNeedRetryException
     * @throws IOException
     */
    public void executeHQL(String hql) throws CommandNeedRetryException, IOException {
        CommandProcessorResponse response = getDriver().run(hql);
        int retCode = response.getResponseCode();
        if (retCode != 0) {
            String err = response.getErrorMessage();
            throw new IOException("Failed to execute hql [" + hql + "], error message is: " + err);
        }
    }

    public void executeHQL(String[] hqls) throws CommandNeedRetryException, IOException {
        for (String sql : hqls)
            executeHQL(sql);
    }

    private HiveMetaStoreClient getMetaStoreClient() throws Exception {
        if (metaStoreClient == null) {
            metaStoreClient = new HiveMetaStoreClient(hiveConf);
        }
        return metaStoreClient;
    }

    public Table getHiveTable(String database, String tableName) throws Exception {
        return getMetaStoreClient().getTable(database, tableName);
    }

    public List<FieldSchema> getHiveTableFields(String database, String tableName) throws Exception {
        return getMetaStoreClient().getFields(database, tableName);
    }

    public String getHiveTableLocation(String database, String tableName) throws Exception {
        Table t = getHiveTable(database, tableName);
        return t.getSd().getLocation();
    }

    public long getFileSizeForTable(Table table) {
        return getBasicStatForTable(new org.apache.hadoop.hive.ql.metadata.Table(table), StatsSetupConst.TOTAL_SIZE);
    }

    public long getFileNumberForTable(Table table) {
        return getBasicStatForTable(new org.apache.hadoop.hive.ql.metadata.Table(table), StatsSetupConst.NUM_FILES);
    }

    public List<String> getHiveDbNames() throws Exception {
        return getMetaStoreClient().getAllDatabases();
    }

    public List<String> getHiveTableNames(String database) throws Exception {
        return getMetaStoreClient().getAllTables(database);
    }

    /**
     * COPIED FROM org.apache.hadoop.hive.ql.stats.StatsUtil for backward compatibility
     * 
     * Get basic stats of table
     * @param table
     *          - table
     * @param statType
     *          - type of stats
     * @return value of stats
     */
    public static long getBasicStatForTable(org.apache.hadoop.hive.ql.metadata.Table table, String statType) {
        Map<String, String> params = table.getParameters();
        long result = 0;

        if (params != null) {
            try {
                result = Long.parseLong(params.get(statType));
            } catch (NumberFormatException e) {
                result = 0;
            }
        }
        return result;
    }

    public boolean isNativeTable(String database, String tableName) throws Exception {
        return !MetaStoreUtils.isNonNativeTable(getMetaStoreClient().getTable(database, tableName));
    }
}
