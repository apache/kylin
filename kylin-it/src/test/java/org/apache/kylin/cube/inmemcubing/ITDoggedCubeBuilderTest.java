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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.inmemcubing2.DoggedCubeBuilder2;
import org.apache.kylin.cube.inmemcubing2.InMemCubeBuilder2;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class ITDoggedCubeBuilderTest extends LocalFileMetadataTestCase {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ITDoggedCubeBuilderTest.class);

    private static final int INPUT_ROWS = 10000;
    private static final int SPLIT_ROWS = 2000;
    private static final int THREADS = 4;

    private static CubeInstance cube;
    private static String flatTable;
    private static Map<TblColRef, Dictionary<String>> dictionaryMap;

    @BeforeClass
    public static void before() throws IOException {
        staticCreateTestMetadata();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);

        cube = cubeManager.getCube("ssb");
        flatTable = LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/data/" + kylinConfig.getHiveIntermediateTablePrefix()
                + "ssb_19920101000000_19920201000000.csv";
        dictionaryMap = ITInMemCubeBuilderTest.getDictionaryMap(cube, flatTable);
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void test() throws Exception {

        ArrayBlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(1000);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        long randSeed = System.currentTimeMillis();

        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(cube.getDescriptor());
        InMemCubeBuilder inmemBuilder = new InMemCubeBuilder(cube.getCuboidScheduler(), flatDesc, dictionaryMap);
        inmemBuilder.setConcurrentThreads(THREADS);
        FileRecordWriter inmemResult = new FileRecordWriter();
        {
            Future<?> future = executorService.submit(inmemBuilder.buildAsRunnable(queue, inmemResult));
            ITInMemCubeBuilderTest.feedData(cube, flatTable, queue, INPUT_ROWS, randSeed);
            future.get();
            inmemResult.close();
        }

        DoggedCubeBuilder doggedBuilder = new DoggedCubeBuilder(cube.getCuboidScheduler(), flatDesc, dictionaryMap);
        doggedBuilder.setConcurrentThreads(THREADS);
        FileRecordWriter doggedResult = new FileRecordWriter();
        {
            Future<?> future = executorService.submit(doggedBuilder.buildAsRunnable(queue, doggedResult));
            ITInMemCubeBuilderTest.feedData(cube, flatTable, queue, INPUT_ROWS, randSeed, SPLIT_ROWS);
            future.get();
            doggedResult.close();
        }

        InMemCubeBuilder2 inmemBuilder2 = new InMemCubeBuilder2(cube.getCuboidScheduler(), flatDesc, dictionaryMap);
        inmemBuilder2.setConcurrentThreads(THREADS);
        FileRecordWriter inmemResult2 = new FileRecordWriter();
        {
            Future<?> future = executorService.submit(inmemBuilder2.buildAsRunnable(queue, inmemResult2));
            ITInMemCubeBuilderTest.feedData(cube, flatTable, queue, INPUT_ROWS, randSeed);
            future.get();
            inmemResult2.close();
        }

        DoggedCubeBuilder2 doggedBuilder2 = new DoggedCubeBuilder2(cube.getCuboidScheduler(), flatDesc, dictionaryMap);
        doggedBuilder2.setConcurrentThreads(THREADS);
        FileRecordWriter doggedResult2 = new FileRecordWriter();
        {
            Future<?> future = executorService.submit(doggedBuilder2.buildAsRunnable(queue, doggedResult2));
            ITInMemCubeBuilderTest.feedData(cube, flatTable, queue, INPUT_ROWS, randSeed, SPLIT_ROWS);
            future.get();
            doggedResult2.close();
        }

        fileCompare(inmemResult.file, inmemResult2.file);
        fileCompare(inmemResult.file, doggedResult.file);
        fileCompare2(inmemResult.file, doggedResult2.file);

        inmemResult.file.delete();
        inmemResult2.file.delete();
        doggedResult.file.delete();
        doggedResult2.file.delete();
    }

    private void fileCompare(File file, File file2) throws IOException {
        BufferedReader r1 = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
        BufferedReader r2 = new BufferedReader(new InputStreamReader(new FileInputStream(file2), "UTF-8"));

        String line1, line2;
        do {
            line1 = r1.readLine();
            line2 = r2.readLine();

            assertEquals(line1, line2);

        } while (line1 != null || line2 != null);

        r1.close();
        r2.close();
    }

    private void fileCompare2(File file, File file2) throws IOException {
        Map<String, Integer> content1 = readContents(file);
        Map<String, Integer> content2 = readContents(file2);
        assertEquals(content1, content2);
    }

    private Map<String, Integer> readContents(File file) throws IOException {
        BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
        Map<String, Integer> content = Maps.newHashMap();
        String line;
        while ((line = r.readLine()) != null) {
            Integer cnt = content.get(line);
            if (cnt == null) {
                cnt = 0;
            }
            content.put(line, cnt + 1);
        }
        r.close();
        return content;
    }

    class FileRecordWriter implements ICuboidWriter {

        File file;
        PrintWriter writer;

        FileRecordWriter() throws IOException {
            file = File.createTempFile("DoggedCubeBuilderTest_", ".data");
            writer = new PrintWriter(file, "UTF-8");
        }

        @Override
        public void write(long cuboidId, GTRecord record) throws IOException {
            writer.print(cuboidId);
            writer.print(", ");
            writer.print(record.toString());
            writer.println();
        }

        @Override
        public void write(long cuboidId, GridTable table) throws IOException {
            writer.print(cuboidId);
            writer.print(", ");
            writer.print(table.toString());
            writer.println();
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {
            writer.close();
        }
    }
}