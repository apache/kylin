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
package org.apache.kylin.query.util;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OBIEEConverterTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testConvertDoubleQuoteSuccess() {
        assertConverted("select * from a where b in (1.0, 1.03E+08, 123.000, 0.00, -1.20, -0.02, -132.00)",
                "select * from a where b in (1, 1.03E+08, 123, 0, -1.20, -0.02, -132)");
        assertConverted("select * from a where a  = 1.0 or b = 123 and c in (1.0, 11) and c = '1.0'",
                "select * from a where a  = 1 or b = 123 and c in (1.0, 11) and c = '1.0'");
        assertConverted("select * from a inner join b where a = 1.0 and b = 2.0",
                "select * from a inner join b where a = 1 and b = 2");
        assertConverted(
                "select * from a where a  = 1.0 union select * from a where a  = 1.0 or b = 123 and c in (1.0, 11)",
                "select * from a where a  = 1 union select * from a where a  = 1.0 or b = 123 and c in (1.0, 11)");
        assertConverted("select *, 1.0 from a where a  = 1.0 group by c",
                "select *, 1.0 from a where a  = 1 group by c");
    }

    public void assertConverted(String original, String expected) {
        Assert.assertEquals(expected, new OBIEEConverter().convert(original, "default", "default"));
    }
}
