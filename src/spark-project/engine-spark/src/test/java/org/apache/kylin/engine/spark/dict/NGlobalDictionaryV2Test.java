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
package org.apache.kylin.engine.spark.dict;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.dict.NBucketDictionary;
import org.apache.spark.dict.NGlobalDictHDFSStore;
import org.apache.spark.dict.NGlobalDictMetaInfo;
import org.apache.spark.dict.NGlobalDictStore;
import org.apache.spark.dict.NGlobalDictionaryV2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.common.DebugFilesystem;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import scala.Tuple2;

public class NGlobalDictionaryV2Test extends NLocalWithSparkSessionTest {

    private final static int BUCKET_SIZE = 10;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        initFs();
    }

    @Override
    public void tearDown() throws Exception {
        DebugFilesystem.assertNoOpenStreams();
        super.tearDown();
    }

    @Test
    public void testGlobalDictHDFSStoreRoundTest() throws IOException {
        testAll();
    }

    @Test
    public void testGlobalDictS3StoreRoundTest() throws IOException {
        // global s3 dict store
        overwriteSystemProp("kylin.engine.global-dict.store.impl", "org.apache.spark.dict.NGlobalDictS3Store");
        testAll();
    }

    private void testAll() throws IOException {
        roundTest(5);
        roundTest(50);
        roundTest(500);
    }

    private void roundTest(int size) throws IOException {
        System.out.println("NGlobalDictionaryV2Test -> roundTest -> " + System.currentTimeMillis());
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NGlobalDictionaryV2 dict1 = new NGlobalDictionaryV2("t1", "a", "spark", config.getHdfsWorkingDirectory());
        NGlobalDictionaryV2 dict2 = new NGlobalDictionaryV2("t2", "a", "local", config.getHdfsWorkingDirectory());
        List<String> stringList = generateRandomData(size);
        Collections.sort(stringList);
        runWithSparkBuildGlobalDict(dict1, stringList);
        runWithLocalBuildGlobalDict(dict2, stringList);
        compareTwoVersionDict(dict1, dict2);
        compareTwoModeVersionNum(dict1, dict2);
    }

    private List<String> generateRandomData(int size) {
        List<String> stringList = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            stringList.add(RandomStringUtils.randomAlphabetic(10));
        }
        return stringList;
    }

    private void runWithSparkBuildGlobalDict(NGlobalDictionaryV2 dict, List<String> stringSet) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        dict.prepareWrite();
        List<Row> rowList = Lists.newLinkedList();
        for (String str : stringSet) {
            rowList.add(RowFactory.create(str));
        }
        Dataset<Row> ds = ss.createDataFrame(rowList,
                new StructType(new StructField[] { DataTypes.createStructField("col1", DataTypes.StringType, true) }));
        ds.toJavaRDD().mapToPair((PairFunction<Row, String, String>) row -> {
            if (row.get(0) == null)
                return new Tuple2<>(null, null);
            return new Tuple2<>(row.get(0).toString(), null);
        }).sortByKey().partitionBy(new HashPartitioner(BUCKET_SIZE)).mapPartitionsWithIndex(
                (Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Object>>) (bucketId, tuple2Iterator) -> {
                    NBucketDictionary bucketDict = dict.loadBucketDictionary(bucketId);
                    while (tuple2Iterator.hasNext()) {
                        Tuple2<String, String> tuple2 = tuple2Iterator.next();
                        bucketDict.addRelativeValue(tuple2._1);
                    }
                    bucketDict.saveBucketDict(bucketId);
                    return Collections.emptyIterator();
                }, true).count();

        dict.writeMetaDict(BUCKET_SIZE, config.getGlobalDictV2MaxVersions(), config.getGlobalDictV2VersionTTL());
    }

    private void runWithLocalBuildGlobalDict(NGlobalDictionaryV2 dict, List<String> stringSet) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        dict.prepareWrite();
        HashPartitioner partitioner = new HashPartitioner(BUCKET_SIZE);
        Map<Integer, List<String>> vmap = new HashMap<>();
        for (int i = 0; i < BUCKET_SIZE; i++) {
            vmap.put(i, Lists.newArrayList());
        }
        for (String string : stringSet) {
            int bucketId = partitioner.getPartition(string);
            vmap.get(bucketId).add(string);
        }

        for (Map.Entry<Integer, List<String>> entry : vmap.entrySet()) {
            NBucketDictionary bucketDict = dict.loadBucketDictionary(entry.getKey());
            for (String s : entry.getValue()) {
                bucketDict.addRelativeValue(s);
            }
            bucketDict.saveBucketDict(entry.getKey());
        }

        dict.writeMetaDict(BUCKET_SIZE, config.getGlobalDictV2MaxVersions(), config.getGlobalDictV2VersionTTL());
    }

    private void compareTwoModeVersionNum(NGlobalDictionaryV2 dict1, NGlobalDictionaryV2 dict2) throws IOException {
        NGlobalDictStore store1 = new NGlobalDictHDFSStore(dict1.getResourceDir());
        NGlobalDictStore store2 = new NGlobalDictHDFSStore(dict2.getResourceDir());
        Assert.assertEquals(store1.listAllVersions().length, store2.listAllVersions().length);
    }

    private void compareTwoVersionDict(NGlobalDictionaryV2 dict1, NGlobalDictionaryV2 dict2) throws IOException {
        NGlobalDictMetaInfo metadata1 = dict1.getMetaInfo();
        NGlobalDictMetaInfo metadata2 = dict2.getMetaInfo();
        // compare dict meta info
        Assert.assertEquals(metadata1, metadata2);

        for (int i = 0; i < metadata1.getBucketSize(); i++) {
            NBucketDictionary bucket1 = dict1.loadBucketDictionary(i);
            NBucketDictionary bucket2 = dict2.loadBucketDictionary(i);

            Object2LongMap<String> map1 = bucket1.getAbsoluteDictMap();
            Object2LongMap<String> map2 = bucket2.getAbsoluteDictMap();
            for (Object2LongMap.Entry<String> entry : map1.object2LongEntrySet()) {
                Assert.assertEquals(entry.getLongValue(), map2.getLong(entry.getKey()));
            }
        }
    }

    private void initFs() {
        DebugFilesystem.clearOpenStreams();
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", DebugFilesystem.class.getCanonicalName());
        conf.set("fs.file.impl.disable.cache", "true");
        HadoopUtil.setCurrentConfiguration(conf);
    }
}
