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
package org.apache.kylin.sdk.datasource.framework.def;

import java.util.Locale;

import org.apache.kylin.common.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class TypeDefTest {
    @Test
    public void testInit() {
        {
            TypeDef t = defineType("DECIMAL(19,4)");
            Assert.assertEquals(19, t.getDefaultPrecision());
            Assert.assertEquals(4, t.getDefaultScale());
            Assert.assertEquals("DECIMAL(19,4)", t.buildString(30, 2));
            Assert.assertEquals("DECIMAL", t.getName());
            Assert.assertEquals(t.getId(), t.getId().toUpperCase(Locale.ROOT));
        }
        {
            TypeDef t = defineType("DECIMAL($p,$s)");
            Assert.assertEquals(-1, t.getDefaultPrecision());
            Assert.assertEquals(-1, t.getDefaultScale());
            Assert.assertEquals("DECIMAL(19,4)", t.buildString(19, 4));
            Assert.assertEquals("DECIMAL", t.getName());
            Assert.assertEquals(t.getId(), t.getId().toUpperCase(Locale.ROOT));
        }
    }

    private TypeDef defineType(String pattern) {
        TypeDef t = new TypeDef();
        t.setId(RandomUtil.randomUUIDStr().toLowerCase(Locale.ROOT));
        t.setExpression(pattern);
        t.init();
        return t;
    }
}
