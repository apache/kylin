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

package org.apache.kylin.storage.hdfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * Created by xiefan on 17-1-10.
 */
public class ITHDFSResourceStoreTest extends HBaseMetadataTestCase {

    KylinConfig kylinConfig;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Ignore
    @Test
    public void testHDFSUrl() throws Exception {
        assertEquals("kylin_default_instance_hdfs@hdfs", kylinConfig.getHDFSMetadataUrl());
        System.out.println("hdfs working dir : " + kylinConfig.getHdfsWorkingDirectory());
    }


    @Ignore
    @Test
    public void testMultiThreadWriteHDFS() throws Exception{
        //System.out.println(kylinConfig.getHdfsWorkingDirectory());
        final Path testDir = new Path("hdfs:///test123");
        final FileSystem fs = HadoopUtil.getFileSystem(testDir);
        final String fileName = "test.json";
        int threadNum = 3;
        ExecutorService service = Executors.newFixedThreadPool(threadNum);
        final CountDownLatch latch = new CountDownLatch(threadNum);
        Path p = new Path(testDir,fileName);
        fs.deleteOnExit(p);
        fs.createNewFile(p);
        for(int i=0;i<threadNum;i++) {
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        long id = Thread.currentThread().getId();
                        Path p = new Path(testDir, fileName);
                        /*while(fs.exists(p)){
                            System.out.println("Thread id : " + id + " can not get lock,sleep a while");
                            Thread.currentThread().sleep(1000);
                        }*/
                        while(!fs.createNewFile(p)){
                            System.out.println("Thread id : " + id + " can not get lock,sleep a while");
                            Thread.currentThread().sleep(1000);
                        }
                        System.out.println("Thread id : " + id + " get lock, sleep a while");
                        Thread.currentThread().sleep(1000);
                        fs.delete(p,true);
                        System.out.println("Thread id : " + id + " release lock");
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        Thread.currentThread().sleep(1000);
        fs.delete(p,true);
        System.out.println("main thread release lock.Waiting threads down");
        System.out.println("file still exist : " + fs.exists(p));
        latch.await();
    }

    @Test
    public void testHDFSStore() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore store = new HDFSResourceStore(config);
        ResourceStoreTest.testAStore(store);
    }

}
