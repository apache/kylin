/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.job;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.lookup.HiveTableReader;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.streaming.cube.IGTRecordWriter;
import org.apache.kylin.streaming.cube.InMemCubeBuilder;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 * This class is going to be deleted
 */
@Ignore("For dev testing")
public class BuildCubeWithStreamTest {

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithStreamTest.class);

    private KylinConfig kylinConfig;
    private CubeManager cubeManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty("hdp.version", "2.2.4.2-2"); // mapred-site.xml ref this
    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);
        DeployUtil.overrideJobJarLocations();

        kylinConfig = KylinConfig.getInstanceFromEnv();
        cubeManager = CubeManager.getInstance(kylinConfig);

    }

    @After
    public void after() {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        CubeInstance cube = cubeManager.getCube("test_kylin_cube_without_slr_left_join_empty");
        final CubeDesc desc = cube.getDescriptor();
        //   cube.getSegments().clear();
        //   cubeManager.updateCube(cube);

        CubeSegment cubeSegment = cube.getSegment("19700101000000_20150401000000", SegmentStatusEnum.NEW);
        Map<TblColRef, Dictionary<?>> dictionaryMap = Maps.newHashMap();

//
        for (DimensionDesc dim : desc.getDimensions()) {
            // dictionary
            for (TblColRef col : dim.getColumnRefs()) {
                if (desc.getRowkey().isUseDictionary(col)) {
                    Dictionary dict = cubeSegment.getDictionary(col);
                    if (dict == null) {
                        throw new IllegalArgumentException("Dictionary for " + col + " was not found.");
                    }
                    logger.info("Dictionary for " + col + " was put into dictionary map.");
                    dictionaryMap.put(col, cubeSegment.getDictionary(col));
                }
            }
        }

//        final String tableName = createIntermediateTable(desc, kylinConfig, null);
        String tableName = "kylin_intermediate_test_kylin_cube_without_slr_desc_19700101000000_20130112000000_a24dec89_efbd_425f_9a5f_8b78dd1412af"; // has 3089 records;
//        tableName = "kylin_intermediate_test_kylin_cube_without_slr_desc_19700101000000_20130112000000_a5e1eb5d_da6b_475d_9807_be0b61f03215"; // only 20 rows;
//        tableName = "kylin_intermediate_test_kylin_cube_without_slr_left_join_desc_19700101000000_20150302000000_0a183367_f245_43d1_8850_1c138c8514c3";
//        tableName = "kylin_intermediate_test_kylin_cube_without_slr_left_join_desc_19700101000000_20150301000000_ce061464_7962_4642_bd7d_7c3d8fbe9389";
        tableName = "kylin_intermediate_test_kylin_cube_without_slr_left_join_desc_19700101000000_20150401000000_fb7ae579_d987_4900_a3b7_c60c731cd269"; // 2 million records
        logger.info("intermediate table name:" + tableName);


        ArrayBlockingQueue queue = new ArrayBlockingQueue<List<String>>(10000);

        InMemCubeBuilder cubeBuilder = new InMemCubeBuilder(queue, cube, dictionaryMap, new ConsoleGTRecordWriter());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(cubeBuilder);

        final Configuration conf = new Configuration();
        HCatInputFormat.setInput(conf, "default", tableName);
        final HCatSchema tableSchema = HCatInputFormat.getTableSchema(conf);
        logger.info(StringUtils.join(tableSchema.getFieldNames(), "\n"));
        HiveTableReader reader = new HiveTableReader("default", tableName);
        List<String> row;
        int counter = 0;
        while (reader.next()) {
            row = reader.getRowAsList();
            queue.put(row);
            counter++;
            if(counter == 200000)
                break;
        }
        queue.put(new ArrayList<String>(0));
        reader.close();

        try {
            future.get();
        } catch (Exception e) {
            logger.error("stream build failed", e);
            throw new IOException("Failed to build cube ", e);
        }

        logger.info("stream build finished");
    }


    private void buildDictionary(List<List<String>> table, CubeDesc desc, Map<TblColRef, Dictionary<?>> dictionaryMap) {
        SetMultimap<TblColRef, String> valueMap = HashMultimap.create();

        List<TblColRef> dimColumns = desc.listDimensionColumnsExcludingDerived();
        for (List<String> row : table) {
            for (int i = 0; i < dimColumns.size(); i++) {
                String cell = row.get(i);
                valueMap.put(dimColumns.get(i), cell);
            }
        }

        for (DimensionDesc dim : desc.getDimensions()) {
            // dictionary
            for (TblColRef col : dim.getColumnRefs()) {
                if (desc.getRowkey().isUseDictionary(col)) {
                    Dictionary dict = DictionaryGenerator.buildDictionaryFromValueList(col.getType(), Collections2.transform(valueMap.get(col), new Function<String, byte[]>() {
                        @Nullable
                        @Override
                        public byte[] apply(String input) {
                            if (input == null)
                                return null;
                            return input.getBytes();
                        }
                    }));

                    logger.info("Building dictionary for " + col);
                    dictionaryMap.put(col, dict);
                }
            }
        }

    }


    class ConsoleGTRecordWriter implements IGTRecordWriter {

        boolean verbose = false;

        @Override
        public void write(Long cuboidId, GTRecord record) throws IOException {
            if (verbose)
                System.out.println(record.toString());
        }
    }
}
