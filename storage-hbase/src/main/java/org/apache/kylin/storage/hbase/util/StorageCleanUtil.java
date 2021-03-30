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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StorageCleanUtil {
    private static final Logger logger = LoggerFactory.getLogger(StorageCleanUtil.class);

    /**
     * this method will close hbaseAdmin after finishing the work.
     */
    public static void dropHTables(final HBaseAdmin hbaseAdmin, List<String> hTables) {
        runSingleThreadTaskQuietly(() -> {
            try {
                for (String htable : hTables) {
                    logger.info("Deleting HBase table {}", htable);

                    if (hbaseAdmin.tableExists(htable)) {
                        if (hbaseAdmin.isTableEnabled(htable)) {
                            hbaseAdmin.disableTable(htable);
                        }

                        hbaseAdmin.deleteTable(htable);
                        logger.info("Deleted HBase table {}", htable);
                    } else {
                        logger.info("HBase table {} does not exist.", htable);
                    }
                }
            } catch (Exception e) {
                // storage cleanup job will delete it
                logger.error("Deleting HBase table failed");
            } finally {
                IOUtils.closeQuietly(hbaseAdmin);
            }
        });
    }

    public static void deleteHDFSPath(final FileSystem fileSystem, List<String> hdfsPaths) {
        runSingleThreadTaskQuietly(() -> {
            try {
                for (String hdfsPath : hdfsPaths) {
                    logger.info("Deleting HDFS path {}", hdfsPath);
                    Path path = new Path(hdfsPath);

                    if (fileSystem.exists(path)) {
                        fileSystem.delete(path, true);
                        logger.info("Deleted HDFS path {}", hdfsPath);
                    }
                }
            } catch (Exception e) {
                // storage cleanup job will delete it
                logger.error("Deleting HDFS path failed");
            }
        });
    }

    private static void runSingleThreadTaskQuietly(Runnable task) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            executorService.execute(task);
        } catch (Exception e) {
            logger.error("Failed to run task", e);
        } finally {
            executorService.shutdown();
        }
    }
}
