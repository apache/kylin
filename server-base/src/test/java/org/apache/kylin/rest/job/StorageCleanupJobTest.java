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

import static org.apache.kylin.common.util.LocalFileMetadataTestCase.cleanAfterClass;
import static org.apache.kylin.common.util.LocalFileMetadataTestCase.staticCreateTestMetadata;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.LocalFileMetadataTestCase.OverlayMetaHook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.Lists;

public class StorageCleanupJobTest {
    @Before
    public void setup() {
        staticCreateTestMetadata(true, new OverlayMetaHook("src/test/resources/ut_meta/storage_ut/"));
    }

    @After
    public void after() {
        cleanAfterClass();
    }

    @Test
    public void test() throws Exception {
        FileSystem mockFs = mock(FileSystem.class);
        prepareUnusedIntermediateHiveTable(mockFs);
        prepareUnusedHDFSFiles(mockFs);

        MockStorageCleanupJob job = new MockStorageCleanupJob(KylinConfig.getInstanceFromEnv(), mockFs, mockFs);
        job.execute(new String[] { "--delete", "true" });

        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        verify(mockFs, times(2)).delete(pathCaptor.capture(), eq(true));
        ArrayList<Path> expected = Lists.newArrayList(
                // verifyCleanUnusedIntermediateHiveTable
                new Path("file:///tmp/examples/test_metadata/kylin-f8edd777-8756-40d5-be19-3159120e4f7b/kylin_intermediate_2838c7fc-722a-48fa-9d1a-8ab37837a952"),

                // verifyCleanUnusedHdfsFiles
                new Path("file:///tmp/examples/test_metadata/kylin-to-be-delete")
        );
        assertEquals(expected, pathCaptor.getAllValues());
    }

    private void prepareUnusedHDFSFiles(FileSystem mockFs) throws IOException {
        Path p1 = new Path("file:///tmp/examples/test_metadata/");
        FileStatus[] statuses = new FileStatus[3];
        FileStatus f1 = mock(FileStatus.class);
        FileStatus f2 = mock(FileStatus.class);
        FileStatus f3 = mock(FileStatus.class);
        // only remove FINISHED and DISCARDED job intermediate files, so this exclude.
        when(f1.getPath()).thenReturn(new Path("kylin-091a0322-249c-43e7-91df-205603ab6883"));
        // remove every segment working dir from deletion list, so this exclude.
        when(f2.getPath()).thenReturn(new Path("kylin-bcf2f125-9b0b-40dd-9509-95ec59b31333"));
        when(f3.getPath()).thenReturn(new Path("kylin-to-be-delete"));
        statuses[0] = f1;
        statuses[1] = f2;
        statuses[2] = f3;

        when(mockFs.listStatus(p1)).thenReturn(statuses);
        Path p2 = new Path("file:///tmp/examples/test_metadata/kylin-to-be-delete");
        when(mockFs.exists(p2)).thenReturn(true);
    }

    private void prepareUnusedIntermediateHiveTable(FileSystem mockFs) throws IOException {
        Path p1 = new Path(
                "file:///tmp/examples/test_metadata/kylin-f8edd777-8756-40d5-be19-3159120e4f7b/kylin_intermediate_2838c7fc-722a-48fa-9d1a-8ab37837a952");
        when(mockFs.exists(p1)).thenReturn(true);
    }

    class MockStorageCleanupJob extends StorageCleanupJob {

        MockStorageCleanupJob(KylinConfig config, FileSystem defaultFs, FileSystem hbaseFs) {
            super(config, defaultFs, hbaseFs);
        }

        @Override
        protected List<String> getHiveTables() throws Exception {
            List<String> l = new ArrayList<>();
            l.add("kylin_intermediate_2838c7fc-722a-48fa-9d1a-8ab37837a952");
            // wrong prefix, so this is exclude.
            l.add("wrong_prefix_6219a647-d8be-49bb-8562-3f4976922a96");
            // intermediate table still in use, so this is exclude.
            l.add("kylin_intermediate_091a0322-249c-43e7-91df-205603ab6883");
            return l;
        }

        @Override
        protected CliCommandExecutor getCliCommandExecutor() throws IOException {
            CliCommandExecutor mockCli = mock(CliCommandExecutor.class);
            when(mockCli.execute((String) notNull())).thenReturn(null);
            return mockCli;
        }
    }
}
