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

package org.apache.kylin.storage.parquet.spark;

import org.apache.kylin.ext.ClassLoaderUtils;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.storage.parquet.spark.gtscanner.ParquetRecordGTScanner;
import org.apache.kylin.storage.parquet.spark.gtscanner.ParquetRecordGTScanner4Cube;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkSubmitter {
    public static final Logger logger = LoggerFactory.getLogger(SparkSubmitter.class);

    public static IGTScanner submitParquetTask(GTScanRequest scanRequest, ParquetPayload payload) {

        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        ParquetTask parquetTask = new ParquetTask(payload);

        ParquetRecordGTScanner scanner = new ParquetRecordGTScanner4Cube(scanRequest.getInfo(), parquetTask.executeTask(), scanRequest,
                payload.getMaxScanBytes());

        return scanner;
    }
}
