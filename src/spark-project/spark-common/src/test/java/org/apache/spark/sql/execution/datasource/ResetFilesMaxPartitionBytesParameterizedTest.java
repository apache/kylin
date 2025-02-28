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

package org.apache.spark.sql.execution.datasource;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

@RunWith(Parameterized.class)
@MetadataInfo
public class ResetFilesMaxPartitionBytesParameterizedTest extends NLocalFileMetadataTestCase {
    protected static final Logger logger = LoggerFactory.getLogger(ResetFilesMaxPartitionBytesParameterizedTest.class);
    private ResetShufflePartition resetShufflePartition = new ResetShufflePartition() {
        public void org$apache$spark$internal$Logging$$log__$eq(Logger msg) {
        }

        public Logger org$apache$spark$internal$Logging$$log_() {
            return logger;
        }
    };

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { KylinConfigBase.FALSE, 0L, 0L, "0", "0", "0", Boolean.FALSE },
                { KylinConfigBase.TRUE, 0L, 1L, "0", "0", "0", Boolean.FALSE },
                { KylinConfigBase.TRUE, 1L, 10L, "0", "20", "0", Boolean.FALSE },
                { KylinConfigBase.TRUE, 1L, 10L, "0", "20", "0", Boolean.FALSE },
                { KylinConfigBase.TRUE, 2L, 1024L * 1024, "3", String.valueOf(1025 * 1024), "2", Boolean.FALSE },
                { KylinConfigBase.TRUE, 2L, 1024L * 1024, "3", String.valueOf(1025 * 1024), "1", Boolean.TRUE } });
    }

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    private final String resetMaxPartitionBytes;
    private final Long totalRowCount;
    private final Long sourceBytes;
    private final String parquetRowCountThresholdSize;
    private final String filesThresholdBytes;
    private final String parquetRowCountPerMb;
    private final Boolean expected;

    public ResetFilesMaxPartitionBytesParameterizedTest(String resetMaxPartitionBytes, Long totalRowCount,
            Long sourceBytes, String parquetRowCountThresholdSize, String filesThresholdBytes,
            String parquetRowCountPerMb, Boolean expected) {
        this.resetMaxPartitionBytes = resetMaxPartitionBytes;
        this.totalRowCount = totalRowCount;
        this.sourceBytes = sourceBytes;
        this.parquetRowCountThresholdSize = parquetRowCountThresholdSize;
        this.filesThresholdBytes = filesThresholdBytes;
        this.parquetRowCountPerMb = parquetRowCountPerMb;
        this.expected = expected;
    }

    @Test
    public void testNeedSetFilesMaxPartitionBytes() {
        overwriteSystemProp("kylin.storage.columnar.reset-max-partition-bytes", resetMaxPartitionBytes);
        overwriteSystemProp("kylin.storage.columnar.parquet-row-count-threshold-size", parquetRowCountThresholdSize);
        overwriteSystemProp("kylin.storage.columnar.files-threshold-bytes", filesThresholdBytes);
        overwriteSystemProp("kylin.storage.columnar.parquet-row-count-per-mb", parquetRowCountPerMb);
        overwriteSystemProp("kylin.storage.columnar.files.max-partition-bytes", "1");
        val kapConfig = KapConfig.getInstanceFromEnv();
        assertEquals(expected,
                resetShufflePartition.needSetFilesMaxPartitionBytes(totalRowCount, sourceBytes, kapConfig));
        val sparkSession = SparkSession.builder().master("local[1]").getOrCreate();
        resetShufflePartition.setFilesMaxPartitionBytes(sourceBytes, totalRowCount, sparkSession);
        if (expected) {
            assertEquals(1L, sparkSession.sessionState().conf().getConf(SQLConf.FILES_MAX_PARTITION_BYTES()));
        } else {
            assertEquals(128L * 1024 * 1024,
                    sparkSession.sessionState().conf().getConf(SQLConf.FILES_MAX_PARTITION_BYTES()));
        }
    }
}
