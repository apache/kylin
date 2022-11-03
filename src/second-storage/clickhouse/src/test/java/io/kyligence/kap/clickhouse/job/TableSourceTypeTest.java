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

package io.kyligence.kap.clickhouse.job;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;


public class TableSourceTypeTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test(expected = RuntimeException.class)
    public void blob() {
        new BlobTableSource().transformFileUrl("wasb://container@account.blob.core.chinacloudapi.cn/blob.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
    }

    @Test(expected = RuntimeException.class)
    public void blobWasbs() {
        new BlobTableSource().transformFileUrl("wasbs://container@account.blob.core.chinacloudapi.cn/blob.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void blobUnsupportedOperationException() {
        new BlobTableSource().transformFileUrl("file://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void hdfs() {
        new HdfsTableSource().transformFileUrl("file://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void unSupportedFormat() {
        new HdfsTableSource().transformFileUrl("file://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
    }

    @Test
    public void hdfsNoException() {
        String hdfsUrl = "hdfs://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet";
        String url = new HdfsTableSource().transformFileUrl(hdfsUrl, "host.docker.internal:9000&test&test123",
                URI.create("/test"));
        Assert.assertEquals("HDFS('" + hdfsUrl + "' , Parquet)", url);
    }

    @Test
    public void viewfs() {
        FileSystemTestHelper.MockFileSystem mockFs = new FileSystemTestHelper.MockFileSystem();
        try{
            when(mockFs.resolvePath(new Path("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet"))).thenReturn(new Path("hdfs://hdfstest/snap.parquet"));
        }catch (IOException e){
            Assert.fail();
        }

        ViewFsTransform viewfs = ViewFsTransform.getInstance();
        Class c = viewfs.getClass();

        try{
            Field field = c.getDeclaredField("vfs");
            field.setAccessible(true);
            field.set(viewfs, mockFs);
        }catch (IllegalAccessException| NoSuchFieldException | SecurityException e){
            Assert.fail();
        }
        
        String result = new HdfsTableSource().transformFileUrl("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
        Assert.assertEquals("HDFS('hdfs://hdfstest/snap.parquet' , Parquet)", result);
    }

    @Test
    public void utSource() {
        val actual = new UtTableSource().transformFileUrl("file://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet", "host.docker.internal:9000&test&test123", URI.create("/test"));
        Assert.assertEquals("URL('host.docker.internal:9000&test&test123//liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet' , Parquet)", actual);
    }
}
