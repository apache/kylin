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

package org.apache.kylin.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class StorageURLTest {

    @Test
    public void testBasic() {
        {
            StorageURL id = new StorageURL("hello@hbase");
            assertEquals("hello", id.getIdentifier());
            assertEquals("hbase", id.getScheme());
            assertEquals(0, id.getAllParameters().size());
            assertEquals("hello@hbase", id.toString());
        }
        {
            StorageURL id = new StorageURL("hello@hbase,a=b,c=d");
            assertEquals("hello", id.getIdentifier());
            assertEquals("hbase", id.getScheme());
            assertEquals(2, id.getAllParameters().size());
            assertEquals("b", id.getParameter("a"));
            assertEquals("d", id.getParameter("c"));
            assertEquals("hello@hbase,a=b,c=d", id.toString());
        }
        {
            StorageURL o = new StorageURL("hello@hbase,c=d");
            StorageURL o2 = new StorageURL("hello@hbase,a=b");
            StorageURL id = o.copy(o2.getAllParameters());
            assertEquals("hello", id.getIdentifier());
            assertEquals("hbase", id.getScheme());
            assertEquals(1, id.getAllParameters().size());
            assertEquals("b", id.getParameter("a"));
            assertEquals("hello@hbase,a=b", id.toString());
            assertEquals("hello@hbase,c=d", o.toString());
            assertEquals("hello@hbase,a=b", o2.toString());
        }
    }

    @Test(expected = NullPointerException.class)
    public void testNullInput() {
        new StorageURL(null);
    }

    @Test
    public void testHDFS() {
        {
            StorageURL id = new StorageURL(
                    "master_ci_instance@hdfs,path=hdfs://sandbox.hortonworks.com:8020/kylin/master_ci_instance/metadata/f112fe00-6f99-4f8e-b075-d57c08501106");

            assertEquals("master_ci_instance", id.getIdentifier());
            assertEquals("hdfs", id.getScheme());
            assertEquals(1, id.getAllParameters().size());
            assertEquals(
                    "master_ci_instance@hdfs,path=hdfs://sandbox.hortonworks.com:8020/kylin/master_ci_instance/metadata/f112fe00-6f99-4f8e-b075-d57c08501106",
                    id.toString());
        }
    }

    @Test
    public void testEdgeCases() {

        {
            StorageURL id = new StorageURL("");
            assertEquals("kylin_metadata", id.getIdentifier());
            assertEquals("", id.getScheme());
            assertEquals(0, id.getAllParameters().size());
            assertEquals("kylin_metadata", id.toString());
        }
        {
            StorageURL id = new StorageURL("hello@");
            assertEquals("hello", id.getIdentifier());
            assertEquals("", id.getScheme());
            assertEquals(0, id.getAllParameters().size());
            assertEquals("hello", id.toString());
        }
        {
            StorageURL id = new StorageURL("hello@hbase,a");
            assertEquals("hello", id.getIdentifier());
            assertEquals("hbase", id.getScheme());
            assertEquals(1, id.getAllParameters().size());
            assertEquals("", id.getParameter("a"));
            assertEquals("hello@hbase,a", id.toString());
        }
    }

    @Test
    public void testValueOfCache() {
        StorageURL id1 = StorageURL.valueOf("hello@hbase");
        StorageURL id2 = StorageURL.valueOf("hello@hbase");
        StorageURL id3 = StorageURL.valueOf("hello @ hbase");
        StorageURL id4 = StorageURL.valueOf("hello@hbase,a=b");
        assertTrue(id1 == id2);
        assertTrue(id1 != id3);
        assertTrue(id1.equals(id3));
        assertTrue(id2 != id4);
        assertTrue(!id2.equals(id4));
    }
}
