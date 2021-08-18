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

package org.apache.kylin.rest.job;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.job.exception.SchedulerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.kylin.common.util.LocalFileMetadataTestCase.cleanAfterClass;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StorageCleanupJobTest {

    private KylinConfig kylinConfig;

    @Before
    public void setup() throws SchedulerException {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() {
        cleanAfterClass();
    }

    @Test
    public void test() throws Exception {
        FileSystem mockFs = mock(FileSystem.class);
        Path basePath = new Path(kylinConfig.getHdfsWorkingDirectory());
        prepareHDFSFiles(basePath, mockFs);

        StorageCleanupJob job = new StorageCleanupJob(kylinConfig, mockFs);
        job.execute(new String[] { "--delete", "true", "--cleanupThreshold", "12" });

        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        verify(mockFs, times(6)).delete(pathCaptor.capture(), eq(true));
        ArrayList<Path> expected = Lists.newArrayList(
                // Verify clean job temp directory
                // new Path(basePath + "/default/job_tmp"),
                // Verify clean unused global dictionary before 12 hour
                new Path(basePath + "/default/dict/global_dict/TEST_KYLIN_FACT/BUYER_ID"),

                // Verify clean unused table snapshot before 12 hour
                new Path(basePath + "/default/table_snapshot/DEFAULT.TEST_COUNTRY/1f2fd967-af50-41c1-a990-8554206b5513"),

                // Verify clean dropped cube
                new Path(basePath + "/default/parquet/dropped_cube"),

                // Verify clean deleted project
                new Path(basePath + "/deleted_project"),

                // Verify clean none used segments
                new Path(basePath + "/default/parquet/ci_left_join_cube/20120101000000_20130101000000_VRC"),
                new Path(basePath + "/default/parquet/ci_left_join_cube/20130101000000_20140101000000_PCN")
        );
        assertEquals(expected, pathCaptor.getAllValues());
    }

    private void prepareHDFSFiles(Path basePath, FileSystem mockFs) throws IOException {

        FileStatus[] segmentStatuses = new FileStatus[2];
        FileStatus segment1 = mock(FileStatus.class);
        FileStatus segment2 = mock(FileStatus.class);

        FileStatus[] cubeStatuses = new FileStatus[3];
        FileStatus cube1 = mock(FileStatus.class);
        FileStatus cube2 = mock(FileStatus.class);
        FileStatus cube3 = mock(FileStatus.class);

        FileStatus[] projectStatuses = new FileStatus[2];
        FileStatus project1 = mock(FileStatus.class);
        FileStatus project2 = mock(FileStatus.class);

        FileStatus[] dictTableStatuses = new FileStatus[1];
        FileStatus dictTable1 = mock(FileStatus.class);

        FileStatus[] dictCloumnStatuses = new FileStatus[3];
        FileStatus dictCloumnStatuses1 = mock(FileStatus.class);
        FileStatus dictCloumnStatuses2 = mock(FileStatus.class);
        FileStatus dictCloumnStatuses3 = mock(FileStatus.class);

        FileStatus[] snapshotTableStatuses = new FileStatus[1];
        FileStatus snapshotTableStatuses1 = mock(FileStatus.class);

        FileStatus[] snapshotStatuses = new FileStatus[3];
        FileStatus snapshotStatuses1 = mock(FileStatus.class);
        FileStatus snapshotStatuses2 = mock(FileStatus.class);
        FileStatus snapshotStatuses3 = mock(FileStatus.class);

        FileStatus[] protectedStatuses = new FileStatus[2];
        FileStatus cubeStatistics = mock(FileStatus.class);
        FileStatus resourcesJdbc = mock(FileStatus.class);

        FileStatus[] allStatuses = new FileStatus[4];

        // Remove job temp directory
        Path jobTmpPath = new Path(basePath + "/default/job_tmp");
        when(mockFs.exists(jobTmpPath)).thenReturn(true);
        when(mockFs.delete(jobTmpPath, true)).thenReturn(true);

        // remove every segment working dir from deletion list, so this exclude.
        when(segment1.getPath()).thenReturn(new Path(basePath + "/default/parquet/ci_left_join_cube/20120101000000_20130101000000_VRC"));
        when(segment2.getPath()).thenReturn(new Path(basePath + "/default/parquet/ci_left_join_cube/20130101000000_20140101000000_PCN"));
        segmentStatuses[0] = segment1;
        segmentStatuses[1] = segment2;

        Path cubePath1 = new Path(basePath + "/default/parquet/ci_left_join_cube");
        Path cubePath2 = new Path(basePath + "/default/parquet/ci_inner_join_cube");
        Path cubePath3 = new Path(basePath + "/default/parquet/dropped_cube");
        when(mockFs.exists(cubePath1)).thenReturn(true);
        when(mockFs.exists(cubePath2)).thenReturn(false);
        when(mockFs.exists(cubePath3)).thenReturn(true);

        when(cube1.getPath()).thenReturn(cubePath1);
        when(cube2.getPath()).thenReturn(cubePath2);
        when(cube3.getPath()).thenReturn(cubePath3);
        cubeStatuses[0] = cube1;
        cubeStatuses[1] = cube2;
        cubeStatuses[2] = cube3;

        when(project1.getPath()).thenReturn(new Path(basePath + "/default"));
        when(project2.getPath()).thenReturn(new Path(basePath + "/deleted_project"));
        projectStatuses[0] = project1;
        projectStatuses[1] = project2;

        Path dictTablePath = new Path(basePath + "/default/dict/global_dict/TEST_KYLIN_FACT");
        when(dictTable1.getPath()).thenReturn(dictTablePath);
        dictTableStatuses[0] = dictTable1;
        when(mockFs.delete(dictTablePath, true)).thenReturn(true);

        Path dictCloumnPath1 = new Path(basePath + "/default/dict/global_dict/TEST_KYLIN_FACT/TEST_COUNT_DISTINCT_BITMAP");
        Path dictCloumnPath2 = new Path(basePath + "/default/dict/global_dict/TEST_KYLIN_FACT/BUYER_ID");
        Path dictCloumnPath3 = new Path(basePath + "/default/dict/global_dict/TEST_KYLIN_FACT/SELLER_ID");
        when(dictCloumnStatuses1.getPath()).thenReturn(dictCloumnPath1);
        when(dictCloumnStatuses2.getPath()).thenReturn(dictCloumnPath2);
        when(dictCloumnStatuses3.getPath()).thenReturn(dictCloumnPath3);
        // It has not been modified for more than 12 hours, but it is still in use, will be not deleted
        when(dictCloumnStatuses1.getModificationTime()).thenReturn(System.currentTimeMillis() - 13 * 3600 * 1000L);
        // It has not been modified for less than 12 hours and unused, will be deleted
        when(dictCloumnStatuses2.getModificationTime()).thenReturn(System.currentTimeMillis() - 13 * 3600 * 1000L);
        // It has not been modified for more than 12 hours and unused, will be not deleted
        when(dictCloumnStatuses3.getModificationTime()).thenReturn(System.currentTimeMillis() - 11 * 3600 * 1000L);
        when(mockFs.delete(dictCloumnPath1, true)).thenReturn(true);
        when(mockFs.delete(dictCloumnPath2, true)).thenReturn(true);
        when(mockFs.delete(dictCloumnPath3, true)).thenReturn(true);
        dictCloumnStatuses[0] = dictCloumnStatuses1;
        dictCloumnStatuses[1] = dictCloumnStatuses2;
        dictCloumnStatuses[2] = dictCloumnStatuses3;

        Path snapshotTablePath = new Path(basePath + "/default/table_snapshot/DEFAULT.TEST_COUNTRY");
        when(snapshotTableStatuses1.getPath()).thenReturn(snapshotTablePath);
        snapshotTableStatuses[0] = snapshotTableStatuses1;
        when(mockFs.delete(snapshotTablePath, true)).thenReturn(true);

        Path snapshotPath1 = new Path(basePath + "/default/table_snapshot/DEFAULT.TEST_COUNTRY/1cb74ab4-0637-407c-8fa9-dbf68eaf9e57");
        Path snapshotPath2 = new Path(basePath + "/default/table_snapshot/DEFAULT.TEST_COUNTRY/1f2fd967-af50-41c1-a990-8554206b5513");
        Path snapshotPath3 = new Path(basePath + "/default/table_snapshot/DEFAULT.TEST_COUNTRY/e137d29b-9231-4305-8c9d-71fcf54bc836");
        when(snapshotStatuses1.getPath()).thenReturn(snapshotPath1);
        when(snapshotStatuses2.getPath()).thenReturn(snapshotPath2);
        when(snapshotStatuses3.getPath()).thenReturn(snapshotPath3);
        // It has not been modified for less than 12 hours and unused, will not be deleted
        when(snapshotStatuses1.getModificationTime()).thenReturn(System.currentTimeMillis() - 11 * 3600 * 1000L);
        // It has not been modified for more than 12 hours and unused, will be deleted
        when(snapshotStatuses2.getModificationTime()).thenReturn(System.currentTimeMillis() - 13 * 3600 * 1000L);
        // It has not been modified for more than 12 hours, but it is still in use, will be not deleted
        when(snapshotStatuses3.getModificationTime()).thenReturn(System.currentTimeMillis() - 13 * 3600 * 1000L);
        snapshotStatuses[0] = snapshotStatuses1;
        snapshotStatuses[1] = snapshotStatuses2;
        snapshotStatuses[2] = snapshotStatuses3;
        when(mockFs.delete(snapshotPath1, true)).thenReturn(true);
        when(mockFs.delete(snapshotPath2, true)).thenReturn(true);
        when(mockFs.delete(snapshotPath3, true)).thenReturn(true);

        Path cubeStatisticsPath = new Path(basePath + "/default/parquet");
        Path resourcesJdbcPath = new Path(basePath + "/deleted_project/parquet");
        when(cubeStatistics.getPath()).thenReturn(cubeStatisticsPath);
        when(resourcesJdbc.getPath()).thenReturn(resourcesJdbcPath);
        protectedStatuses[0] = cubeStatistics;
        protectedStatuses[1] = resourcesJdbc;
        when(mockFs.delete(cubeStatisticsPath, true)).thenReturn(true);
        when(mockFs.delete(resourcesJdbcPath, true)).thenReturn(true);

        allStatuses[0] = project1;
        allStatuses[1] = project2;
        allStatuses[2] = cubeStatistics;
        allStatuses[3] = resourcesJdbc;

        Path defaultProjectParquetPath = new Path(basePath + "/default/parquet");
        Path deletedProjectParquetPath = new Path(basePath + "/deleted_project/parquet");
        when(mockFs.exists(defaultProjectParquetPath)).thenReturn(true);
        when(mockFs.exists(deletedProjectParquetPath)).thenReturn(true);

        when(mockFs.exists(basePath)).thenReturn(true);
        when(mockFs.listStatus(new Path(basePath + "/default/parquet/ci_left_join_cube"))).thenReturn(segmentStatuses);
        when(mockFs.listStatus(defaultProjectParquetPath)).thenReturn(cubeStatuses);
        when(mockFs.listStatus(basePath)).thenReturn(allStatuses);
        when(mockFs.listStatus(basePath, StorageCleanupJob.pathFilter)).thenReturn(projectStatuses);
        Path dictPath = new Path(basePath + "/default/dict/global_dict");
        when(mockFs.exists(dictPath)).thenReturn(true);
        when(mockFs.listStatus(dictPath)).thenReturn(dictTableStatuses);
        when(mockFs.listStatus(new Path(dictPath + "/TEST_KYLIN_FACT"))).thenReturn(dictCloumnStatuses);
        Path snapshotPath = new Path(basePath + "/default/table_snapshot");
        when(mockFs.exists(snapshotPath)).thenReturn(true);
        when(mockFs.listStatus(snapshotPath)).thenReturn(snapshotTableStatuses);
        when(mockFs.listStatus(new Path(snapshotPath + "/DEFAULT.TEST_COUNTRY"))).thenReturn(snapshotStatuses);
    }
}
