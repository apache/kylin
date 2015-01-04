package com.kylinolap.metadata.tool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

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

public class HiveClient {

    protected static HiveConf hiveConf = null;
    protected static Driver driver = null;
    protected static HiveMetaStoreClient metaStoreClient = null;

    private static HiveClient instance = null;

    private HiveClient() {
        setup();
    }

    public static HiveClient getInstance() {

        if (instance == null) {
            synchronized (HiveClient.class) {
                if (instance == null)
                    instance = new HiveClient();
            }

        }

        return instance;
    }

    private void setup() {
        hiveConf = new HiveConf(HiveSourceTableLoader.class);
        driver = new Driver(hiveConf);
        try {
            metaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        SessionState.start(new CliSessionState(hiveConf));
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    /**
     * Get the hive ql driver to execute ddl or dml
     * @return
     */
    public Driver getDriver() {
        return driver;
    }

    /**
     * Get the Hive Meta store client;
     * @return
     */
    public HiveMetaStoreClient getMetaStoreClient() {
        return metaStoreClient;
    }

    public ReaderContext getReaderContext(String database, String table) throws MetaException, CommandNeedRetryException, IOException, ClassNotFoundException {

        Iterator<Entry<String, String>> itr = hiveConf.iterator();
        Map<String, String> map = new HashMap<String, String>();
        while (itr.hasNext()) {
            Entry<String, String> kv = itr.next();
            map.put(kv.getKey(), kv.getValue());
        }

        ReaderContext readCntxt = runsInMaster(map, database, table);

        return readCntxt;
    }

    private ReaderContext runsInMaster(Map<String, String> config, String database, String table) throws HCatException {
        ReadEntity entity = new ReadEntity.Builder().withDatabase(database).withTable(table).build();
        HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
        ReaderContext cntxt = reader.prepareRead();
        return cntxt;
    }

    public HCatReader getHCatReader(ReaderContext cntxt, int slaveNum) throws HCatException {

        HCatReader reader = DataTransferFactory.getHCatReader(cntxt, slaveNum);
        return reader;

    }

}
