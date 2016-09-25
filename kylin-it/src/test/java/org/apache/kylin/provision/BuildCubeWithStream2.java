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

package org.apache.kylin.provision;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

/**
 *  for streaming cubing case "test_streaming_table", using multiple threads to build it concurrently.
 */
public class BuildCubeWithStream2 extends BuildCubeWithStream {

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithStream2.class);
    private boolean generateData = true;

    @Override
    public void build() throws Exception {
        clearSegment(cubeName);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        final long date1 = 0;
        final long date2 = f.parse("2013-01-01").getTime();

        new Thread(new Runnable() {
            @Override
            public void run() {

                Random rand = new Random();
                while (generateData == true) {
                    try {
                        generateStreamData(date1, date2, rand.nextInt(100));
                        sleep(rand.nextInt(rand.nextInt(100 * 1000))); // wait random time, from 0 to 100 seconds
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        List<FutureTask<ExecutableState>> futures = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            Thread.sleep(2 * 60 * 1000); // sleep 2 mintues
            FutureTask futureTask = new FutureTask(new Callable<ExecutableState>() {
                @Override
                public ExecutableState call() {
                    ExecutableState result = null;
                    try {
                        result = buildSegment(cubeName, 0, Long.MAX_VALUE);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return result;
                }
            });

            executorService.submit(futureTask);
            futures.add(futureTask);
        }

        generateData = false; // stop generating message to kafka
        executorService.shutdown();
        int succeedBuild = 0;
        for (int i = 0; i < futures.size(); i++) {
            ExecutableState result = futures.get(i).get(20, TimeUnit.MINUTES);
            logger.info("Checking building task " + i + " whose state is " + result);
            Assert.assertTrue(result == null || result == ExecutableState.SUCCEED || result == ExecutableState.DISCARDED );
            if (result == ExecutableState.SUCCEED)
                succeedBuild++;
        }

        logger.info(succeedBuild + " build jobs have been successfully completed.");
        List<CubeSegment> segments = cubeManager.getCube(cubeName).getSegments(SegmentStatusEnum.READY);
        Assert.assertTrue(segments.size() == succeedBuild);

    }

    public static void main(String[] args) throws Exception {
        try {
            beforeClass();

            BuildCubeWithStream2 buildCubeWithStream = new BuildCubeWithStream2();
            buildCubeWithStream.before();
            buildCubeWithStream.build();
            logger.info("Build is done");
            buildCubeWithStream.after();
            afterClass();
            logger.info("Going to exit");
            System.exit(0);
        } catch (Exception e) {
            logger.error("error", e);
            System.exit(1);
        }

    }

}
