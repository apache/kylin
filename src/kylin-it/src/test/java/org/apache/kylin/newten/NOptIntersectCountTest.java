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

package org.apache.kylin.newten;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.storage.ParquetStorage;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.datasource.storage.StorageStore;
import org.apache.spark.sql.datasource.storage.StorageStoreFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;

public class NOptIntersectCountTest extends NLocalWithSparkSessionTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        setOverlay("src/test/resources/ut_meta/opt_intersect_count");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/opt_intersect_count" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "opt_intersect_count";
    }

    @Test
    public void testOptIntersectCountBuild() throws Exception {
        String dfName = "c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e";
        fullBuild(dfName);
        NDataSegment seg = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(dfName)
                .getLatestReadySegment();
        NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), 100001);
        ParquetStorage storage = new ParquetStorage();
        StorageStore storageStore = StorageStoreFactory.create(seg.getModel().getStorageType());
        List<Row> rows = storage.getFrom(storageStore.getStoragePath(seg, dataCuboid.getLayoutId()), ss)
                .collectAsList();
        Assert.assertEquals(9, rows.size());

        String ret = rows.stream().map(row -> {
            List<String> list = new ArrayList<>();

            for (int i = 0; i < 4; i++) {
                list.add(row.get(i).toString());
            }
            list.add(String.valueOf(getCountDistinctValue((byte[]) (row.get(4)))));
            return list;
        }).collect(Collectors.toList()).toString();

        Assert.assertEquals("[[18, Shenzhen, male, handsome, 1]," //
                + " [18, Shenzhen, male, rich, 1]," //
                + " [18, Shenzhen, male, tall, 1]," //
                + " [19, Beijing, female, handsome, 1]," //
                + " [19, Beijing, female, rich, 1]," //
                + " [19, Beijing, female, tall, 2]," //
                + " [20, Shanghai, male, handsome, 2]," //
                + " [20, Shanghai, male, rich, 2]," //
                + " [20, Shanghai, male, tall, 1]]", ret);
    }

    private int getCountDistinctValue(byte[] bytes) {
        Roaring64NavigableMap bitMap = new Roaring64NavigableMap();
        try {
            bitMap.deserialize(new KryoDataInput(new Input((bytes))));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bitMap.getIntCardinality();
    }

    @Test
    public void testIntersectCountQuery() throws Exception {
        /*
        Source table: USER_ID, AGE, CITY, TAG
        TAG value split by "|"
        
        group by key: 20,Shanghai
        
        rich, 2
        tall, 1
        handsome, 2
        ====================================
        
        group by key: 19,Beijing
        
        rich, 1
        tall, 2
        handsome, 1
        ====================================
        
        group by key: 18,Shenzhen
        
        rich, 1
        tall, 1
        handsome, 1
        * */
        fullBuild("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        String query1 = "select AGE, CITY, " + "intersect_count(USER_ID, TAG, array['rich','tall','handsome']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r1 = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,0", r1.get(0));
        Assert.assertEquals("19,Beijing,0", r1.get(1));
        Assert.assertEquals("20,Shanghai,1", r1.get(2));

        String query2 = "select AGE, CITY, intersect_count(USER_ID, TAG, array['rich']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r2 = ExecAndComp.queryModel(getProject(), query2).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,1", r2.get(0));
        Assert.assertEquals("19,Beijing,1", r2.get(1));
        Assert.assertEquals("20,Shanghai,2", r2.get(2));

        String query3 = "select AGE, CITY, intersect_count(USER_ID, TAG, array['tall']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r3 = ExecAndComp.queryModel(getProject(), query3).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,1", r3.get(0));
        Assert.assertEquals("19,Beijing,2", r3.get(1));
        Assert.assertEquals("20,Shanghai,1", r3.get(2));

        String query4 = "select AGE, CITY, intersect_count(USER_ID, TAG, array['handsome']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r4 = ExecAndComp.queryModel(getProject(), query4).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,1", r4.get(0));
        Assert.assertEquals("19,Beijing,1", r4.get(1));
        Assert.assertEquals("20,Shanghai,2", r4.get(2));

        String query5 = "select AGE, CITY, intersect_count(USER_ID, TAG, array['rich', 'tall']) "
                + "from TEST_INTERSECT_COUNT group by AGE, CITY";
        List<String> r5 = ExecAndComp.queryModel(getProject(), query5).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("18,Shenzhen,0", r5.get(0));
        Assert.assertEquals("19,Beijing,1", r5.get(1));
        Assert.assertEquals("20,Shanghai,1", r5.get(2));

        String query6 = "select CITY, intersect_count(USER_ID, TAG, array['rich', 'tall']) "
                + "from TEST_INTERSECT_COUNT group by CITY";
        List<String> r6 = ExecAndComp.queryModel(getProject(), query6).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());

        Assert.assertEquals("Beijing,1", r6.get(0));
        Assert.assertEquals("Shanghai,1", r6.get(1));
        Assert.assertEquals("Shenzhen,0", r6.get(2));
    }
}
