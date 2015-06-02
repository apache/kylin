package org.apache.kylin.job.streaming;

import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeBuilder;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.streaming.MicroBatchCondition;
import org.apache.kylin.streaming.StreamBuilder;
import org.apache.kylin.streaming.StreamMessage;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

/**
 */
@Ignore
public class CubeStreamConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(CubeStreamConsumerTest.class);

    private KylinConfig kylinConfig;

    private static final String CUBE_NAME = "test_kylin_cube_without_slr_left_join_ready";

    @BeforeClass
    public static void beforeClass() throws Exception {
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty("hdp.version", "2.2.0.0-2041"); // mapred-site.xml ref this
    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);

        kylinConfig = KylinConfig.getInstanceFromEnv();
        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();
        final CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(CUBE_NAME);
        CubeBuilder cubeBuilder = new CubeBuilder(cube);
        cubeBuilder.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        // remove all existing segments
        CubeManager.getInstance(kylinConfig).updateCube(cubeBuilder);

    }

    @Test
    public void test() throws Exception {
        LinkedBlockingDeque<StreamMessage> queue = new LinkedBlockingDeque<>();
        StreamBuilder cubeStreamBuilder = new StreamBuilder(queue, new MicroBatchCondition(Integer.MAX_VALUE, 30 * 1000), new CubeStreamConsumer(CUBE_NAME));
        final Future<?> future = Executors.newSingleThreadExecutor().submit(cubeStreamBuilder);
        loadDataFromLocalFile(queue, 100000);
        future.get();
    }

    private void loadDataFromLocalFile(BlockingQueue<StreamMessage> queue, final int maxCount) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("../table.txt")));
        String line;
        int count = 0;
        while ((line = br.readLine()) != null && count++ < maxCount) {
            final List<String> strings = Arrays.asList(line.split("\t"));
            queue.put(new StreamMessage(System.currentTimeMillis(), StringUtils.join(",", strings).getBytes()));
        }
        queue.put(StreamMessage.EOF);
    }
}
