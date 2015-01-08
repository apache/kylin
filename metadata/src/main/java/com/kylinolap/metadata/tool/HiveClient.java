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

package com.kylinolap.metadata.tool;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

public class HiveClient {

    protected HiveConf hiveConf = null;
    protected Driver driver = null;
    protected HiveMetaStoreClient metaStoreClient = null;

    public HiveClient() {
        hiveConf = new HiveConf(HiveClient.class);
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
     * 
     * @param hql
     * @throws CommandNeedRetryException
     * @throws IOException
     */
    public void executeHQL(String hql) throws CommandNeedRetryException, IOException {
        int retCode = getDriver().run(hql).getResponseCode();
        if (retCode != 0) {
            throw new IOException("Failed to execute hql [" + hql + "], return code from hive driver : [" + retCode + "]");
        }
    }
    
    public void executeHQL(String[] hqls) throws CommandNeedRetryException, IOException {
        for(String sql: hqls)
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


    public HCatReader getHCatReader(ReaderContext cntxt, int slaveNum) throws HCatException {

        HCatReader reader = DataTransferFactory.getHCatReader(cntxt, slaveNum);
        return reader;

    }

}
