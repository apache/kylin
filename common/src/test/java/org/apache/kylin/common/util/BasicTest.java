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

package org.apache.kylin.common.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
* Created by honma on 10/17/14.
* <p/>
* Keep this test case to test basic java functionality
* development concept proving use
*/
@Ignore("convenient trial tool for dev")
@SuppressWarnings("unused")
public class BasicTest {
    protected static final org.slf4j.Logger log = LoggerFactory.getLogger(BasicTest.class);

    private void log(ByteBuffer a) {
        Integer x = 4;
        foo(x);
    }

    private void foo(Long a) {
        System.out.printf("a");

    }

    private void foo(Integer b) {
        System.out.printf("b");
    }

    private enum MetricType {
        Count, DimensionAsMetric, DistinctCount, Normal
    }

    @Test
    @Ignore("convenient trial tool for dev")
    public void test1() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        ListenableFuture futureTask = MoreExecutors.listeningDecorator(executorService).submit(new Runnable() {
            @Override
            public void run() {
                
            }
        });
        futureTask.addListener(new Runnable() {
            @Override
            public void run() {

            }
        }, executorService);

    }

    @Test
    @Ignore("fix it later")
    public void test2() throws IOException, ConfigurationException {
        int m = 1 << 15;
        System.out.println(m);
    }
}
