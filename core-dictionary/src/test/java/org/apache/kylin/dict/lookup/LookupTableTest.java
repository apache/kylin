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

package org.apache.kylin.dict.lookup;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dict.TrieDictionaryForest;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class LookupTableTest extends LocalFileMetadataTestCase {

    private KylinConfig config = null;

    private LookupTable<String> lookupTable;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        config = KylinConfig.getInstanceFromEnv();
        lookupTable = initLookupTable();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testScan() throws Exception {
        List<String> values = new ArrayList<String>();
        values.add(millis("2012-01-24"));
        values.add(millis("2012-12-30"));
        List<String> results = lookupTable.scan("CAL_DT", values, "YEAR_BEG_DT");

        Assert.assertTrue(results.size() > 0);
        for (String i : results) {
            System.out.println(i);

            Assert.assertEquals(millis("2012-01-01"), i);
        }
    }

    @Test
    public void testMapRange() throws Exception {
        Pair<String, String> results = lookupTable.mapRange("CAL_DT", millis("2012-01-24"), millis("2012-12-30"), "QTR_BEG_DT");

        Assert.assertTrue(results != null);
        System.out.println("The first qtr_beg_dt is " + results.getFirst());
        System.out.println("The last qtr_beg_dt is " + results.getSecond());

        Assert.assertEquals(millis("2012-01-01"), results.getFirst());
        Assert.assertEquals(millis("2012-10-01"), results.getSecond());
    }

    @Test
    public void testMapRange2() throws Exception {
        Pair<String, String> results = lookupTable.mapRange("WEEK_BEG_DT", millis("2013-05-01"), millis("2013-08-01"), "CAL_DT");

        System.out.println(DateFormat.formatToDateStr(Long.parseLong(results.getFirst())));
        System.out.println(DateFormat.formatToDateStr(Long.parseLong(results.getSecond())));

        Assert.assertEquals(millis("2013-05-05"), results.getFirst());
        Assert.assertEquals(millis("2013-08-03"), results.getSecond());
    }

    @Test
    public void testMapValues() throws Exception {
        Set<String> values = new HashSet<String>();
        values.add(millis("2012-01-24"));
        values.add(millis("2012-12-30"));
        Set<String> results = lookupTable.mapValues("CAL_DT", values, "YEAR_BEG_DT");

        Assert.assertTrue(results.size() == 1);
        for (String i : results) {
            System.out.println(i);

            Assert.assertEquals(millis("2012-01-01"), i);
        }
    }

    @Test
    public void testGetClassName(){
        String name = TrieDictionaryForest.class.getName();
        System.out.println(name);

    }

    private String millis(String dateStr) {
        return String.valueOf(DateFormat.stringToMillis(dateStr));
    }

    public LookupTable<String> initLookupTable() throws Exception {

        MetadataManager metaMgr = MetadataManager.getInstance(config);

        String tableName = "EDW.TEST_CAL_DT";
        String[] pkCols = new String[] { "CAL_DT" };
        String snapshotResPath = "/table_snapshot/TEST_CAL_DT.csv/4af48c94-86de-4e22-a4fd-c49b06cbaa4f.snapshot";
        SnapshotTable snapshot = getSnapshotManager().getSnapshotTable(snapshotResPath);
        TableDesc tableDesc = metaMgr.getTableDesc(tableName);
        LookupTable<String> lt = new LookupStringTable(tableDesc, pkCols, snapshot);

        System.out.println(lt);

        return lt;
    }

    private SnapshotManager getSnapshotManager() {
        return SnapshotManager.getInstance(config);
    }

}
