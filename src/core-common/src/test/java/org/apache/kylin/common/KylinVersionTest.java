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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KylinVersionTest {
    @Test
    public void testNormal() {
        KylinVersion ver1 = new KylinVersion("2.1.0");
        Assertions.assertEquals(2, ver1.major);
        Assertions.assertEquals(1, ver1.minor);
        Assertions.assertEquals(0, ver1.revision);
    }

    @Test
    void testNoRevision() {
        KylinVersion ver1 = new KylinVersion("2.1");
        Assertions.assertEquals(2, ver1.major);
        Assertions.assertEquals(1, ver1.minor);
        Assertions.assertEquals(0, ver1.revision);
    }

    @Test
    void testToString() {
        KylinVersion ver1 = new KylinVersion("2.1.7.321");
        Assertions.assertEquals(2, ver1.major);
        Assertions.assertEquals(1, ver1.minor);
        Assertions.assertEquals(7, ver1.revision);
        Assertions.assertEquals(321, ver1.internal);
        Assertions.assertEquals("2.1.7.321", ver1.toString());
    }

    @Test
    void testCompare() {
        Assertions.assertTrue(KylinVersion.isBefore200("1.9.9"));
        Assertions.assertFalse(KylinVersion.isBefore200("2.0.0"));
        Assertions.assertTrue(new KylinVersion("2.1.0").compareTo(new KylinVersion("2.1.0.123")) < 0);
    }
}
