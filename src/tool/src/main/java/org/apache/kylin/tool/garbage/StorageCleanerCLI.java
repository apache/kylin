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
package org.apache.kylin.tool.garbage;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.util.concurrent.MoreExecutors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageCleanerCLI {

    public static void main(String[] args) {
        System.out.println("start to cleanup HDFS.");
        try {
            log.info("Init cleaning task thread pool as the direct executor service.");
            CleanTaskExecutorService.getInstance().bindWorkingPool(MoreExecutors::newDirectExecutorService);

            StorageCleaner cleaner = new StorageCleaner().withTag(StorageCleaner.CleanerTag.CLI);
            CleanTaskExecutorService.getInstance().submit(cleaner,
                    KylinConfig.getInstanceFromEnv().getStorageCleanTaskTimeout(), TimeUnit.MILLISECONDS).get();
        } catch (Exception e) {
            log.warn("cleanup HDFS failed.", e);
        }
        log.info("cleanup HDFS finished.");
        System.out.println("cleanup HDFS finished.");
        Unsafe.systemExit(0);
    }
}
