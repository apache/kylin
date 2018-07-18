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

package org.apache.kylin.cube;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CubeDescManagerTest {

    @Test
    public void testNullProcessLogic() throws Exception {
        String encoding = null;
        if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
            assertEquals(null, encoding);
        } else if (encoding.startsWith("dict")) {
            assertFalse(encoding.startsWith("dict"));
        } else {
            assertEquals("dict", encoding);
        }
    }

    @Test
    public void testDictProcessLogic() throws Exception {
        String encoding = "dict";
        if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
            assertEquals("dict", encoding);
        } else if (encoding.startsWith("dict")) {
            assertFalse(encoding.startsWith("dict"));
        } else {
            assertEquals("dict", encoding);
        }
    }

    @Test
    public void testStartDictProcessLogic() throws Exception {
        String encoding = "dict(v1)";
        if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
            assertEquals(null, encoding);
        } else if (encoding.startsWith("dict")) {
            assertTrue(encoding.startsWith("dict"));
        } else {
            assertEquals("dict", encoding);
        }
    }

    @Test
    public void testNonDictProcessLogic() throws Exception {
        String encoding = "boolean";
        if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
            assertEquals(null, encoding);
        } else if (encoding.startsWith("dict")) {
            assertTrue(encoding.startsWith("dict"));
        } else {
            assertEquals("boolean", encoding);
        }
    }
}
