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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.time.FastDateFormat;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * <p/>
 * Keep this test case to test basic java functionality
 * development concept proving use
 */
@Ignore("convenient trial tool for dev")
@SuppressWarnings("unused")
public class BasicTest {
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(BasicTest.class);

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

    public static int counter = 1;

    class X {
        byte[] mm = new byte[100];

        public X() {
            counter++;
        }
    }

    @Test
    public void testxx() throws InterruptedException {
        System.out.println( 0x8fL);
        byte[] space = new byte[100];
        ByteBuffer buffer = ByteBuffer.wrap(space, 10, 20);
        buffer.put((byte) 1);
    }

    @Test
    public void testyy() throws InterruptedException {
        long wallClock = System.currentTimeMillis();

        HashMap<Integer, byte[]> map = Maps.newHashMap();
        for (int i = 0; i < 10000000; i++) {
            byte[] a = new byte[100];
            map.put(i, a);
        }

        System.out.println("Time Consumed: " + (System.currentTimeMillis() - wallClock));
    }

    @Test
    public void test0() throws Exception {

        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<?>> futures = Lists.newArrayList();

        futures.add(executorService.submit(new Runnable() {
            @Override
            public void run() {
                throw new RuntimeException("hi");
            }
        }));

        futures.add(executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                    System.out.println("finish 1");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }));

        try {
            for (Future<?> future : futures) {
                future.get(1, TimeUnit.HOURS);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.out.println(e.getMessage());
        }

        futures.clear();

        futures.add(executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    System.out.println("finish 2");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }));

        try {
            for (Future<?> future : futures) {
                future.get(1, TimeUnit.HOURS);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    @Ignore("convenient trial tool for dev")
    public void test1() throws Exception {

        System.out.println(org.apache.kylin.common.util.DateFormat.formatToTimeStr(1433833611000L));
        System.out.println(org.apache.kylin.common.util.DateFormat.formatToTimeStr(1433250517000L));
        System.out.println(org.apache.kylin.common.util.DateFormat.stringToMillis("2015-06-01 00:00:00"));
        System.out.println(org.apache.kylin.common.util.DateFormat.stringToMillis("2015-05-15 17:00:00"));

        String bb = "\\x00\\x00\\x00\\x00\\x01\\x3F\\xD0\\x2D\\58\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00";//2013/07/12 07:59:37
        String cc = "\\x00\\x00\\x00\\x00\\x01\\x41\\xBE\\x8F\\xD8\\x00\\x00\\x00\\x00\\x00\\x00\\x00";//2013/10/16 08:00:00
        String dd = "\\x00\\x00\\x00\\x00\\x01\\x41\\xBE\\x8F\\xD8\\x07\\x00\\x18\\x00\\x00\\x00";

        byte[] bytes = BytesUtil.fromReadableText(dd);
        long ttt = BytesUtil.readLong(bytes, 2, 8);
        System.out.println(time(ttt));

        System.out.println("\\");
        System.out.println("n");

        System.out.println("The start key is set to " + null);
        long current = System.currentTimeMillis();
        System.out.println(time(current));

        Calendar a = Calendar.getInstance();
        Calendar b = Calendar.getInstance();
        Calendar c = Calendar.getInstance();
        b.clear();
        c.clear();

        System.out.println(time(b.getTimeInMillis()));
        System.out.println(time(c.getTimeInMillis()));

        a.setTimeInMillis(current);
        b.set(a.get(Calendar.YEAR), a.get(Calendar.MONTH), a.get(Calendar.DAY_OF_MONTH), a.get(Calendar.HOUR_OF_DAY), a.get(Calendar.MINUTE));
        c.set(a.get(Calendar.YEAR), a.get(Calendar.MONTH), a.get(Calendar.DAY_OF_MONTH), a.get(Calendar.HOUR_OF_DAY), 0);

        System.out.println(time(b.getTimeInMillis()));
        System.out.println(time(c.getTimeInMillis()));

    }

    @Test
    @Ignore("fix it later")
    public void test2() throws IOException {
        ArrayList<String> x = Lists.newArrayListWithCapacity(10);
        x.set(2, "dd");
        for (String y : x) {
            System.out.println(y);
        }
    }

    @Test
    @Ignore("for dev only")
    public void test3() throws Exception {
        FastDateFormat formatter = org.apache.kylin.common.util.DateFormat.getDateFormat("MMM dd, yyyy hh:mm:ss aa");
        System.out.println(formatter.format(new Date()));

        String timeStr = "Jul 20, 2016 9:59:17 AM";

        System.out.println(formatter.parse(timeStr).getTime());
    }

    private static String time(long t) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return dateFormat.format(cal.getTime());
    }
}
