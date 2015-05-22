/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.kylin.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.lookup.FileTableReader;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.streaming.cube.IGTRecordWriter;
import org.apache.kylin.streaming.cube.InMemCubeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class InMemCubeBuilderTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(InMemCubeBuilderTest.class);

    private KylinConfig kylinConfig;
    private CubeManager cubeManager;

    @Before
    public void before() {
        createTestMetadata();
        
        kylinConfig = KylinConfig.getInstanceFromEnv();
        cubeManager = CubeManager.getInstance(kylinConfig);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }
    
    @Test
    public void test() throws Exception {
        final CubeInstance cube = cubeManager.getCube("test_kylin_cube_without_slr_left_join_empty");
        final String flatTable = "../examples/test_case_data/localmeta/data/flatten_data_for_without_slr_left_join.csv";

        Map<TblColRef, Dictionary<?>> dictionaryMap = getDictionaryMap(cube, flatTable);
        ArrayBlockingQueue<List<String>> queue = new ArrayBlockingQueue<List<String>>(10000);

        InMemCubeBuilder cubeBuilder = new InMemCubeBuilder(queue, cube, dictionaryMap, new ConsoleGTRecordWriter());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(cubeBuilder);

        CubeJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), null);
        int nColumns = flatTableDesc.getColumnList().size();
        FileTableReader reader = new FileTableReader(flatTable, nColumns);
        
        while (reader.next()) {
            String[] row = reader.getRow();
            queue.put(Arrays.asList(row));
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

    private Map<TblColRef, Dictionary<?>> getDictionaryMap(CubeInstance cube, String flatTable) throws IOException {
        Map<TblColRef, Dictionary<?>> result = Maps.newHashMap();
        CubeDesc desc = cube.getDescriptor();
        CubeJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(desc, null);
        int nColumns = flatTableDesc.getColumnList().size();

        List<TblColRef> columns = Cuboid.getBaseCuboid(desc).getColumns();
        for (int c = 0; c < columns.size(); c++) {
            TblColRef col = columns.get(c);
            if (desc.getRowkey().isUseDictionary(col)) {
                logger.info("Building dictionary for " + col);
                List<byte[]> valueList = readValueList(flatTable, nColumns, flatTableDesc.getRowKeyColumnIndexes()[c]);
                Dictionary<?> dict = DictionaryGenerator.buildDictionaryFromValueList(col.getType(), valueList);
                result.put(col, dict);
            }
        }
        return result;
    }

    private List<byte[]> readValueList(String flatTable, int nColumns, int c) throws IOException {
        List<byte[]> result = Lists.newArrayList();
        FileTableReader reader = new FileTableReader(flatTable, nColumns);
        while (reader.next()) {
            String[] row = reader.getRow();
            if (row[c] != null) {
                result.add(Bytes.toBytes(row[c]));
            }
        }
        reader.close();
        return result;
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