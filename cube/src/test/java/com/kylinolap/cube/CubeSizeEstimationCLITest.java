package com.kylinolap.cube;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.estimation.CubeSizeEstimationCLI;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.metadata.MetadataManager;

/**
 * Created by honma on 9/1/14.
 */
public class CubeSizeEstimationCLITest extends LocalFileMetadataTestCase {

    String cubeName = "test_kylin_cube_with_slr_ready";
    long[] cardinality;
    CubeDesc cubeDesc;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.removeInstance(this.getTestConfig());

        String cubeName = "test_kylin_cube_with_slr_ready";
        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        cubeDesc = cubeInstance.getDescriptor();
        cardinality = new long[] { 100, 100, 100, 10000, 1000, 100, 100, 100, 100 };

    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void test() {
        long[] x = new long[] { 5, 8, 5, 115, 122, 127, 137, 236, 101 };
        CubeSizeEstimationCLI.estimatedCubeSize(cubeName, x);
    }

    @Test
    public void baseCuboidTest() {
        long cuboidID = getCuboidID(0, 1, 2, 3, 4, 5, 6, 7, 8);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (10000000000000000L * (32 + 39));
    }

    @Test
    public void cuboidTest1() {
        long cuboidID = getCuboidID(0, 1, 2, 4, 5, 6, 7, 8);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (1000000000000000L * (32 + 37));
    }

    @Test
    public void cuboidTest2() {

        long cuboidID = getCuboidID(0);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (100L * (1 + 32));
    }

    @Test
    public void cuboidTest3() {
        long cuboidID = getCuboidID(4, 5);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (1000L * (3 + 32));
    }

    @Test
    public void cuboidTest4() {
        long cuboidID = getCuboidID(4, 5, 6);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (100000L * (4 + 32));
    }

    private long getCuboidID(int... bitIndice) {
        long ret = 0;
        long mask = 1L;
        for (int index : bitIndice) {
            ret |= mask << index;
        }
        return ret;
    }
}
