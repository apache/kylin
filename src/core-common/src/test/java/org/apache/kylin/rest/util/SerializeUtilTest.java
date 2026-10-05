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

package org.apache.kylin.rest.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InvalidClassException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.NativeQueryRealization;
import org.junit.jupiter.api.Test;

class SerializeUtilTest {

    @Test
    void testRoundTripAllowedCollections() {
        List<String> list = Arrays.asList("kylin", "cache");
        assertEquals(list, SerializeUtil.deserialize(SerializeUtil.serialize(list)));

        Map<String, Long> map = new HashMap<>();
        map.put("scanRows", 42L);
        assertEquals(map, SerializeUtil.deserialize(SerializeUtil.serialize(map)));
    }

    @Test
    void testRoundTripAllowedKylinType() {
        NativeQueryRealization realization = new NativeQueryRealization("model-id", 7L, "table");
        Object deserialized = SerializeUtil.deserialize(SerializeUtil.serialize(realization));
        assertTrue(deserialized instanceof NativeQueryRealization);
        assertEquals("model-id", ((NativeQueryRealization) deserialized).getModelId());
    }

    @Test
    void testRoundTripArrays() {
        int[] ints = { 1, 2, 3 };
        assertArrayEquals(ints, (int[]) SerializeUtil.deserialize(SerializeUtil.serialize(ints)));

        String[] strings = { "kylin" };
        assertArrayEquals(strings, (String[]) SerializeUtil.deserialize(SerializeUtil.serialize(strings)));
    }

    @Test
    void testRejectClassOutsideAllowList() {
        byte[] bytes = SerializeUtil.serialize(new File("/tmp/kylin"));
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> SerializeUtil.deserialize(bytes));
        assertTrue(error.getCause() instanceof InvalidClassException);
    }

    @Test
    void testAllowedClassNames() {
        assertTrue(SerializeUtil.isAllowedClassName("org.apache.kylin.rest.response.SQLResponse"));
        assertTrue(SerializeUtil.isAllowedClassName("java.util.ArrayList"));
        assertTrue(SerializeUtil.isAllowedClassName("java.lang.String"));
        assertTrue(SerializeUtil.isAllowedClassName("java.math.BigDecimal"));
        assertTrue(SerializeUtil.isAllowedClassName("[B"));
        assertTrue(SerializeUtil.isAllowedClassName("[[Ljava.lang.String;"));
    }

    @Test
    void testRejectedClassNames() {
        assertFalse(SerializeUtil.isAllowedClassName("java.io.File"));
        assertFalse(SerializeUtil.isAllowedClassName("org.apache.commons.collections.functors.InvokerTransformer"));
        assertFalse(SerializeUtil.isAllowedClassName("javax.management.BadAttributeValueExpException"));
        assertFalse(SerializeUtil.isAllowedClassName("com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl"));
    }
}
