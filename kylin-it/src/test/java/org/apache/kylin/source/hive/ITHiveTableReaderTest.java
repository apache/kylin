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

package org.apache.kylin.source.hive;

import java.io.IOException;

import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test case need the hive runtime; Please run it with sandbox;
 * @author shaoshi
 *
 * It is in the exclude list of default profile in pom.xml
 */
public class ITHiveTableReaderTest extends HBaseMetadataTestCase {

    @Test
    public void test() throws IOException {
        HiveTableReader reader = new HiveTableReader("default", "test_kylin_fact");
        int rowNumber = 0;
        while (reader.next()) {
            String[] row = reader.getRow();
            Assert.assertEquals(9, row.length);
            //System.out.println(ArrayUtils.toString(row));
            rowNumber++;
        }

        reader.close();
        Assert.assertEquals(10000, rowNumber);
    }
}
