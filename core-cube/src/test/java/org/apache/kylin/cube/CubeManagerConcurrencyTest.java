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

package org.apache.kylin.cube;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class CubeManagerConcurrencyTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CubeManagerConcurrencyTest.class);
    
    @Before
    public void setUp() throws Exception {
        System.setProperty("kylin.cube.max-building-segments", "10000");
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        System.clearProperty("kylin.cube.max-building-segments");
    }

    @Test
    public void test() throws Exception {
        final KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(config);

        // 4 empty new cubes to start with
        final String[] cubeNames = { "c1", "c2", "c3", "c4" };
        final int n = cubeNames.length;
        final int updatesPerCube = 100;
        final CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        final List<CubeInstance> cubes = new ArrayList<>();
        for (String name : cubeNames) {
            cubes.add(cubeMgr.createCube(name, ProjectInstance.DEFAULT_PROJECT_NAME, desc, null));
        }

        final AtomicInteger runningFlag = new AtomicInteger();
        final Vector<Exception> exceptions = new Vector<>();

        // 1 thread, keeps reloading cubes
        Thread reloadThread = new Thread() {
            @Override
            public void run() {
                try {
                    Random rand = new Random();
                    while (runningFlag.get() == 0) {
                        String name = cubeNames[rand.nextInt(n)];
                        CubeManager.getInstance(config).reloadCube(name);
                        Thread.sleep(1);
                    }
                } catch (Exception ex) {
                    logger.error("reload thread error", ex);
                    exceptions.add(ex);
                }
            }
        };
        reloadThread.start();

        // 4 threads, keeps updating cubes
        Thread[] updateThreads = new Thread[n];
        for (int i = 0; i < n; i++) {
            // each thread takes care of one cube
            // for now, the design refuses concurrent updates to one cube
            final String cubeName = cubeNames[i];
            updateThreads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        Random rand = new Random();
                        for (int i = 0; i < updatesPerCube; i++) {
                            CubeManager mgr = CubeManager.getInstance(config);
                            CubeInstance cube = mgr.getCube(cubeName);
                            mgr.appendSegment(cube, new TSRange((long) i, (long) i + 1));
                            Thread.sleep(rand.nextInt(1));
                        }
                    } catch (Exception ex) {
                        logger.error("update thread ", ex);
                        exceptions.add(ex);
                    }
                }
            };
            updateThreads[i].start();
        }

        // wait things done
        for (int i = 0; i < n; i++) {
            updateThreads[i].join();
        }
        runningFlag.incrementAndGet();
        reloadThread.join();
        
        // check result and error
        if (exceptions.isEmpty() == false) {
            logger.error(exceptions.size() + " exceptions encountered, see logs above");
            fail();
        }
        for (int i = 0; i < n; i++) {
            CubeInstance cube = cubeMgr.getCube(cubeNames[i]);
            assertEquals(updatesPerCube, cube.getSegments().size());
        }
    }

}
