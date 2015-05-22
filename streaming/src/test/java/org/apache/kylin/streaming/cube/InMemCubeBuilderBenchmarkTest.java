package org.apache.kylin.streaming.cube;

import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 */
public class InMemCubeBuilderBenchmarkTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(InMemCubeBuilderBenchmarkTest.class);

    private static final int BENCHMARK_RECORD_LIMIT = 2000000;
    private static final String CUBE_NAME = "test_kylin_cube_with_slr_1_new_segment";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    private Map<TblColRef, Dictionary<?>> getDictionaryMap(CubeSegment cubeSegment) {
        final Map<TblColRef, Dictionary<?>> dictionaryMap = Maps.newHashMap();
        final CubeDesc desc = cubeSegment.getCubeDesc();
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
        return dictionaryMap;
    }

    private static class ConsoleGTRecordWriter implements IGTRecordWriter {

        boolean verbose = false;

        @Override
        public void write(Long cuboidId, GTRecord record) throws IOException {
            if (verbose)
                System.out.println(record.toString());
        }
    }

    private void loadDataFromLocalFile(LinkedBlockingQueue queue) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("../table.txt")));
        String line;
        int counter = 0;
        while ((line = br.readLine()) != null) {
            queue.put(Arrays.asList(line.split("\t")));
            counter++;
            if (counter == BENCHMARK_RECORD_LIMIT) {
                break;
            }
        }
        queue.put(Collections.emptyList());
    }

    private void loadDataFromRandom(LinkedBlockingQueue queue) throws IOException, InterruptedException {
        queue.put(Collections.emptyList());
    }


    @Test
    public void test() throws Exception {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        final CubeInstance cube = cubeManager.getCube(CUBE_NAME);
        final CubeSegment cubeSegment = cube.getFirstSegment();

        LinkedBlockingQueue queue = new LinkedBlockingQueue<List<String>>();

        InMemCubeBuilder cubeBuilder = new InMemCubeBuilder(queue, cube, getDictionaryMap(cubeSegment), new ConsoleGTRecordWriter());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(cubeBuilder);
        loadDataFromLocalFile(queue);
        future.get();
        logger.info("stream build finished");
    }
}
