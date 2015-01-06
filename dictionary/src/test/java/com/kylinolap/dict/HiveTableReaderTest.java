/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.dict;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.dict.lookup.HiveTableReader;

/**
 * This test case need the hive runtime; Please run it with sandbox; It is in the exclude list of default profile in pom.xml
 * @author shaoshi
 *
 */
public class HiveTableReaderTest extends HBaseMetadataTestCase {

    @Test
    public void test() throws IOException {
        HiveTableReader reader = new HiveTableReader("default", "test_kylin_fact");
        int rowNumber = 0;
        while (reader.next()) {
            String[] row = reader.getRow();
            Assert.assertEquals(9, row.length);
            System.out.println(ArrayUtils.toString(row));
            rowNumber++;
        }

        Assert.assertEquals(10000, rowNumber);
        reader.close();
    }
}
