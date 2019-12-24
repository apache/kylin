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

package org.apache.kylin.engine.spark;

import com.google.common.collect.Maps;
import io.kyligence.kap.engine.spark.job.UdfManager;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.BeforeClass;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public class LocalWithSparkSessionTest extends LocalFileMetadataTestCase implements Serializable {

    private Map<String, String> systemProp = Maps.newHashMap();
    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");

        ss = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        KylinSparkEnv.setSparkSession(ss);
        UdfManager.create(ss);

        System.out.println("Check spark sql config [spark.sql.catalogImplementation = "
                + ss.conf().get("spark.sql.catalogImplementation") + "]");
    }

    protected ExecutableState wait(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            ExecutableState status = job.getStatus();
            if (!status.isProgressing()) {
                return status;
            }
        }
    }

    public String getProject() {
        return "default";
    }
}
