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

package org.apache.kylin.engine.mr.steps;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.inmemcubing.DoggedCubeBuilder;
import org.apache.kylin.cube.inmemcubing.InputConverterUnitForRawData;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CuboidSchedulerUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;
import org.powermock.reflect.Whitebox;

@PrepareForTest({ MRUtil.class, CuboidSchedulerUtil.class, InMemCuboidMapper.class })
public class InMemCuboidMapperTest extends LocalFileMetadataTestCase {
    @Rule
    public PowerMockRule rule = new PowerMockRule();

    private String cubeName;
    private CubeInstance cube;
    private InMemCuboidMapper<NullWritable> inMemCuboidMapper;
    private MapDriver<NullWritable, Object, ByteArrayWritable, ByteArrayWritable> mapDriver;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        FileUtils.deleteDirectory(new File("./meta"));
        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl().toString()), new File("./meta"));

        cubeName = "test_kylin_cube_with_slr_1_new_segment";
        cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        inMemCuboidMapper = new InMemCuboidMapper<>();
        mapDriver = MapDriver.newMapDriver(inMemCuboidMapper);

        PowerMockito.stub(PowerMockito.method(CuboidSchedulerUtil.class, "getCuboidSchedulerByMode", CubeSegment.class,
                String.class)).toReturn(cube.getCuboidScheduler());
        IMRBatchCubingInputSide mockInputSide = createMockInputSide();
        PowerMockito.stub(PowerMockito.method(MRUtil.class, "getBatchCubingInputSide")).toReturn(mockInputSide);

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteDirectory(new File("./meta"));
    }

    @Test
    public void testMapper() throws Exception {
        TestHandler testHandler = new TestHandler();
        setConfigurationAndMock(testHandler);

        mapDriver.setInput(NullWritable.get(), NullWritable.get());
        mapDriver.run();
    }

    @Test
    public void testMapperWithCutRow() throws Exception {
        Whitebox.setInternalState(inMemCuboidMapper, "splitRowThreshold", 1);
        Whitebox.setInternalState(inMemCuboidMapper, "unitRows", 1);
        TestHandler testHandler = new TestWithCutRowHandler();
        setConfigurationAndMock(testHandler);

        mapDriver.setInput(NullWritable.get(), NullWritable.get());
        mapDriver.run();
    }

    private void setConfigurationAndMock(TestHandler testHandler) throws Exception {
        Configuration configuration = mapDriver.getConfiguration();
        configuration.set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, "100");
        configuration.set(BatchConstants.CFG_CUBE_NAME, cubeName);
        configuration.set(BatchConstants.CFG_CUBE_SEGMENT_ID, "198va32a-a33e-4b69-83dd-0bb8b1f8c53b");

        DoggedCubeBuilder mockDoggedCubeBuilder = createMockDoggedCubeBuilder(testHandler);
        PowerMockito.whenNew(DoggedCubeBuilder.class).withAnyArguments().thenReturn(mockDoggedCubeBuilder);
    }

    private IMRBatchCubingInputSide createMockInputSide() throws Exception {
        IMRInput.IMRTableInputFormat mockInputFormat = createMockInputFormat();
        IMRBatchCubingInputSide mockInputSide = PowerMockito.mock(IMRBatchCubingInputSide.class);
        PowerMockito.when(mockInputSide.getFlatTableInputFormat()).thenReturn(mockInputFormat);
        return mockInputSide;
    }

    private IMRTableInputFormat createMockInputFormat() throws Exception {
        String[] row = getMockInputRow();
        Collection<String[]> rows = new ArrayList<>();
        rows.add(row);
        IMRTableInputFormat mockFormat = PowerMockito.mock(IMRTableInputFormat.class);
        PowerMockito.when(mockFormat, "parseMapperInput", Mockito.any()).thenReturn(rows);
        return mockFormat;
    }

    private static String[] getMockInputRow() {
        return new String[] { "2012-01-01" };
    }

    private DoggedCubeBuilder createMockDoggedCubeBuilder(final TestHandler hanlder) throws Exception {
        DoggedCubeBuilder mockDoggedCubeBuilder = PowerMockito.mock(DoggedCubeBuilder.class);
        PowerMockito.when(mockDoggedCubeBuilder, "buildAsRunnable", Mockito.any(BlockingQueue.class), Mockito.any(),
                Mockito.any()).thenAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        hanlder.setInputQueue((BlockingQueue) args[0]);
                        return hanlder;
                    }
                });
        return mockDoggedCubeBuilder;
    }

    private static class TestHandler implements Runnable {
        BlockingQueue queue;

        public void setInputQueue(BlockingQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                consumeRows();

                while (true) {
                    String[] newRow = (String[]) queue.take();
                    if (InputConverterUnitForRawData.END_ROW == newRow) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        protected void consumeRows() throws InterruptedException {
            String[] row = (String[]) queue.take();
            assertArrayEquals(getMockInputRow(), row);
        }

    }

    private static class TestWithCutRowHandler extends TestHandler {

        protected void consumeRows() throws InterruptedException {
            String[] row = (String[]) queue.take();
            assertArrayEquals(getMockInputRow(), row);

            row = (String[]) queue.take();
            assertArrayEquals(InputConverterUnitForRawData.CUT_ROW, row);
        }

    }
}
