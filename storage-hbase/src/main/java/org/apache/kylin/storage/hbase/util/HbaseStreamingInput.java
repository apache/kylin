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

package org.apache.kylin.storage.hbase.util;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class HbaseStreamingInput {
    private static final Logger logger = LoggerFactory.getLogger(HbaseStreamingInput.class);

    private static final int CELL_SIZE = 128 * 1024; // 128 KB
    private static final byte[] CF = "F".getBytes();
    private static final byte[] QN = "C".getBytes();

    public static void createTable(String tableName) throws IOException {
        Connection conn = getConnection();
        Admin hadmin = conn.getAdmin();

        try {
            boolean tableExist = hadmin.tableExists(TableName.valueOf(tableName));
            if (tableExist) {
                logger.info("HTable '" + tableName + "' already exists");
                return;
            }

            logger.info("Creating HTable '" + tableName + "'");
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            desc.setValue(HTableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());//disable region split
            desc.setMemStoreFlushSize(512 << 20);//512M

            HColumnDescriptor fd = new HColumnDescriptor(CF);
            fd.setBlocksize(CELL_SIZE);
            desc.addFamily(fd);
            hadmin.createTable(desc);

            logger.info("HTable '" + tableName + "' created");
        } finally {
            IOUtils.closeQuietly(conn);
            IOUtils.closeQuietly(hadmin);
        }
    }

    private static void scheduleJob(Semaphore semaphore, int interval) {
        while (true) {
            semaphore.release();
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void addData(String tableName) throws IOException {

        createTable(tableName);

        final Semaphore semaphore = new Semaphore(0);
        new Thread(new Runnable() {
            @Override
            public void run() {
                scheduleJob(semaphore, 300000);//5 minutes a batch
            }
        }).start();

        while (true) {
            try {
                semaphore.acquire();
                int waiting = semaphore.availablePermits();
                if (waiting > 0) {
                    logger.warn("There are another " + waiting + " batches waiting to be added");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }

            Connection conn = getConnection();
            Table table = conn.getTable(TableName.valueOf(tableName));

            byte[] key = new byte[8 + 4];//time + id

            logger.info("============================================");
            long startTime = System.currentTimeMillis();
            logger.info("data load start time in millis: " + startTime);
            logger.info("data load start at " + formatTime(startTime));
            List<Put> buffer = Lists.newArrayList();
            for (int i = 0; i < (1 << 10); ++i) {
                long time = System.currentTimeMillis();
                Bytes.putLong(key, 0, time);
                Bytes.putInt(key, 8, i);
                Put put = new Put(key);
                byte[] cell = randomBytes(CELL_SIZE);
                put.addColumn(CF, QN, cell);
                buffer.add(put);
            }
            table.put(buffer);
            table.close();
            conn.close();
            long endTime = System.currentTimeMillis();
            logger.info("data load end at " + formatTime(endTime));
            logger.info("data load time consumed: " + (endTime - startTime));
            logger.info("============================================");
        }
    }

    public static void randomScan(String tableName) throws IOException {

        final Semaphore semaphore = new Semaphore(0);
        new Thread(new Runnable() {
            @Override
            public void run() {
                scheduleJob(semaphore, 60000);//1 minutes a batch
            }
        }).start();

        while (true) {
            try {
                semaphore.acquire();
                int waiting = semaphore.drainPermits();
                if (waiting > 0) {
                    logger.warn("Too many queries to handle! Blocking " + waiting + " sets of scan requests");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }

            Random r = new Random();
            Connection conn = getConnection();
            Table table = conn.getTable(TableName.valueOf(tableName));

            long leftBound = getFirstKeyTime(table);
            long rightBound = System.currentTimeMillis();

            for (int t = 0; t < 5; ++t) {
                long start = (long) (leftBound + r.nextDouble() * (rightBound - leftBound));
                long end = start + 600000;//a period of 10 minutes
                logger.info("A scan from " + formatTime(start) + " to " + formatTime(end));

                Scan scan = new Scan();
                scan.setStartRow(Bytes.toBytes(start));
                scan.setStopRow(Bytes.toBytes(end));
                scan.addFamily(CF);
                ResultScanner scanner = table.getScanner(scan);
                long hash = 0;
                int rowCount = 0;
                for (Result result : scanner) {
                    Cell cell = result.getColumnLatestCell(CF, QN);
                    byte[] value = cell.getValueArray();
                    if (cell.getValueLength() != CELL_SIZE) {
                        logger.error("value size invalid!!!!!");
                    }

                    hash += Arrays.hashCode(Arrays.copyOfRange(value, cell.getValueOffset(), cell.getValueLength() + cell.getValueOffset()));
                    rowCount++;
                }
                scanner.close();
                logger.info("Scanned " + rowCount + " rows, the (meaningless) hash for the scan is " + hash);
            }
            table.close();
            conn.close();
        }
    }

    private static long getFirstKeyTime(Table table) throws IOException {
        long startTime = 0;

        Scan scan = new Scan();
        scan.addFamily(CF);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell cell = result.getColumnLatestCell(CF, QN);
            byte[] key = cell.getRowArray();
            startTime = Bytes.toLong(key, cell.getRowOffset(), 8);
            logger.info("Retrieved first record time: " + formatTime(startTime));
            break;//only get first one
        }
        scanner.close();
        return startTime;

    }

    private static Connection getConnection() throws IOException {
        return HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
    }

    private static String formatTime(long time) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        return dateFormat.format(cal.getTime());
    }

    private static byte[] randomBytes(int lenth) {
        byte[] bytes = new byte[lenth];
        Random rand = new Random();
        rand.nextBytes(bytes);
        return bytes;
    }

    public static void main(String[] args) throws Exception {

        if (args[0].equalsIgnoreCase("createtable")) {
            createTable(args[1]);
        } else if (args[0].equalsIgnoreCase("adddata")) {
            addData(args[1]);
        } else if (args[0].equalsIgnoreCase("randomscan")) {
            randomScan(args[1]);
        }
    }

}
