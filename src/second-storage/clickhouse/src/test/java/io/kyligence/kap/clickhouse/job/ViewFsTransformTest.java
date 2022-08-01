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

import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.exception.KylinException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.mockito.Mockito.when;

public class ViewFsTransformTest {

    @Test
    public void testGetInstance() {
        ViewFsTransform viewfs = ViewFsTransform.getInstance();
        Assert.assertNotNull(viewfs);
    }

    @Test
    public void testGenerateFileUrl() {
        FileSystemTestHelper.MockFileSystem mockFs = new FileSystemTestHelper.MockFileSystem();
        try{
            when(mockFs.resolvePath(new Path("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet"))).thenReturn(new Path("hdfs://hdfstest/snap.parquet"));
        }catch (IOException e){
            Assert.fail();
        }

        ViewFsTransform viewfs = ViewFsTransform.getInstance();
        Assert.assertNotNull(viewfs);
        Class c = viewfs.getClass();

        try{
            Field field = c.getDeclaredField("vfs");
            field.setAccessible(true);
            field.set(viewfs, mockFs);
        }catch (IllegalAccessException| NoSuchFieldException | SecurityException e){
            Assert.fail();
        }

        String url = ViewFsTransform.getInstance().generateFileUrl("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet");
        Assert.assertEquals("hdfs://hdfstest/snap.parquet", url);
    }

    @Test(expected = KylinException.class)
    public void testGenerateFileFailUrl() {
        ViewFsTransform.getInstance().generateFileUrl("viewfs://cluster/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet");
    }
}