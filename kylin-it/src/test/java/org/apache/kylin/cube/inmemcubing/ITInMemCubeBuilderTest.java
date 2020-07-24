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

package org.apache.kylin.cube.inmemcubing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.IterableDictionaryValueEnumerator;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class ITInMemCubeBuilderTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(ITInMemCubeBuilderTest.class);

    private CubeInstance cube;
    private String flatTable;
    private Map<TblColRef, Dictionary<String>> dictionaryMap;

    private int nInpRows;
    private int nThreads;

    @Before
    public void before() throws IOException {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testSSBCubeMore() throws Exception {
        testBuild("ssb", //
                LOCALMETA_TEST_DATA + "/data/" + KylinConfig.getInstanceFromEnv().getHiveIntermediateTablePrefix()
                        + "ssb_19920101000000_19920201000000.csv",
                7000, 4);
    }

    @Test
    public void testSSBCube() throws Exception {
        testBuild("ssb", //
                LOCALMETA_TEST_DATA + "/data/" + KylinConfig.getInstanceFromEnv().getHiveIntermediateTablePrefix()
                        + "ssb_19920101000000_19920201000000.csv",
                1000, 1);
    }

    public void testBuild(String cubeName, String flatTable, int nInpRows, int nThreads) throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);

        this.nInpRows = nInpRows;
        this.nThreads = nThreads;

        this.cube = cubeManager.getCube(cubeName);
        this.flatTable = flatTable;
        this.dictionaryMap = getDictionaryMap(cube, flatTable);

        testBuildInner();
    }

    private void testBuildInner() throws Exception {

        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(cube.getDescriptor());
        InMemCubeBuilder cubeBuilder = new InMemCubeBuilder(cube.getCuboidScheduler(), flatDesc, dictionaryMap);
        //DoggedCubeBuilder cubeBuilder = new DoggedCubeBuilder(cube.getDescriptor(), dictionaryMap);
        cubeBuilder.setConcurrentThreads(nThreads);

        ArrayBlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(1000);
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            // round 1
            {
                Future<?> future = executorService.submit(cubeBuilder.buildAsRunnable(queue, new ConsoleGTRecordWriter()));
                feedData(cube, flatTable, queue, nInpRows);
                future.get();
            }

            // round 2, zero input
            {
                Future<?> future = executorService.submit(cubeBuilder.buildAsRunnable(queue, new ConsoleGTRecordWriter()));
                feedData(cube, flatTable, queue, 0);
                future.get();
            }

            // round 3
            {
                Future<?> future = executorService.submit(cubeBuilder.buildAsRunnable(queue, new ConsoleGTRecordWriter()));
                feedData(cube, flatTable, queue, nInpRows);
                future.get();
            }

        } catch (Exception e) {
            logger.error("stream build failed", e);
            throw new IOException("Failed to build cube ", e);
        }
    }

    static void feedData(final CubeInstance cube, final String flatTable, ArrayBlockingQueue<String[]> queue, int count)
            throws IOException, InterruptedException {
        feedData(cube, flatTable, queue, count, 0);
    }

    static void feedData(final CubeInstance cube, final String flatTable, ArrayBlockingQueue<String[]> queue, int count,
            long randSeed) throws IOException, InterruptedException {
        feedData(cube, flatTable, queue, count, randSeed, Integer.MAX_VALUE);
    }

    static void feedData(final CubeInstance cube, final String flatTable, ArrayBlockingQueue<String[]> queue, int count,
            long randSeed, int splitRowThreshold) throws IOException, InterruptedException {
        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(cube.getDescriptor());
        int nColumns = flatDesc.getAllColumns().size();

        @SuppressWarnings("unchecked")
        Set<String>[] distinctSets = new Set[nColumns];
        for (int i = 0; i < nColumns; i++)
            distinctSets[i] = new TreeSet<String>();

        // get distinct values on each column
        List<String> lines = FileUtils.readLines(new File(flatTable), "UTF-8");
        for (String line : lines) {
            String[] row = StringUtil.splitByComma(line.trim());
            assert row.length == nColumns;
            for (int i = 0; i < nColumns; i++)
                distinctSets[i].add(row[i]);
        }

        List<String[]> distincts = new ArrayList<String[]>();
        for (int i = 0; i < nColumns; i++) {
            distincts.add((String[]) distinctSets[i].toArray(new String[distinctSets[i].size()]));
        }

        Random rand = new Random();
        if (randSeed != 0)
            rand.setSeed(randSeed);

        // output with random data
        int countOfLastSplit = 0;
        for (; count > 0; count--) {
            String[] row = new String[nColumns];
            for (int i = 0; i < nColumns; i++) {
                String[] candidates = distincts.get(i);
                row[i] = candidates[rand.nextInt(candidates.length)];
            }
            queue.put(row);

            // put cut row if possible
            countOfLastSplit++;
            if (countOfLastSplit >= splitRowThreshold) {
                queue.put(InputConverterUnitForRawData.CUT_ROW);
                countOfLastSplit = 0;
            }
        }
        queue.put(InputConverterUnitForRawData.END_ROW);
    }

    static Map<TblColRef, Dictionary<String>> getDictionaryMap(CubeInstance cube, String flatTable) throws IOException {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        CubeDesc desc = cube.getDescriptor();
        CubeJoinedFlatTableEnrich flatDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(desc), desc);
        int nColumns = flatDesc.getAllColumns().size();

        List<TblColRef> columns = Cuboid.getBaseCuboid(desc).getColumns();
        for (int c = 0; c < columns.size(); c++) {
            TblColRef col = columns.get(c);
            if (desc.getRowkey().isUseDictionary(col)) {
                logger.info("Building dictionary for " + col);
                List<String> valueList = readValueList(flatTable, nColumns, flatDesc.getRowKeyColumnIndexes()[c]);
                Dictionary<String> dict = DictionaryGenerator.buildDictionary(col.getType(), new IterableDictionaryValueEnumerator(valueList));
                result.put(col, dict);
            }
        }

        for (int measureIdx = 0; measureIdx < cube.getDescriptor().getMeasures().size(); measureIdx++) {
            MeasureDesc measureDesc = cube.getDescriptor().getMeasures().get(measureIdx);
            FunctionDesc func = measureDesc.getFunction();
            List<TblColRef> dictCols = func.getMeasureType().getColumnsNeedDictionary(func);
            if (dictCols.isEmpty())
                continue;

            int[] flatTableIdx = flatDesc.getMeasureColumnIndexes()[measureIdx];
            List<TblColRef> paramCols = func.getParameter().getColRefs();
            for (int i = 0; i < paramCols.size(); i++) {
                TblColRef col = paramCols.get(i);
                if (dictCols.contains(col)) {
                    int colIdxOnFlat = flatTableIdx[i];
                    logger.info("Building dictionary for " + col);
                    List<String> valueList = readValueList(flatTable, nColumns, colIdxOnFlat);
                    Dictionary<String> dict = DictionaryGenerator.buildDictionary(col.getType(), new IterableDictionaryValueEnumerator(valueList));

                    result.put(col, dict);
                }
            }
        }

        return result;
    }

    private static List<String> readValueList(String flatTable, int nColumns, int c) throws IOException {
        List<String> result = Lists.newArrayList();
        List<String> lines = FileUtils.readLines(new File(flatTable), "UTF-8");
        for (String line : lines) {
            String[] row = StringUtil.splitByComma(line.trim());
            if (row.length != nColumns) {
                throw new IllegalStateException();
            }
            if (row[c] != null) {
                result.add(row[c]);
            }
        }
        return result;
    }

    class ConsoleGTRecordWriter implements ICuboidWriter {

        boolean verbose = false;

        @Override
        public void write(long cuboidId, GTRecord record) throws IOException {
            if (verbose)
                System.out.println(record.toString());
        }

        @Override
        public void write(long cuboidId, GridTable table) throws IOException {
            if (verbose)
                System.out.println(table.toString());
        }

        @Override
        public void flush() {
            if (verbose) {
                System.out.println("flush");
            }
        }

        @Override
        public void close() {
            if (verbose) {
                System.out.println("close");
            }
        }
    }
}