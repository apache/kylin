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

package org.apache.kylin.common.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class CaseInsensitiveStringCollectionTest {
    @Test
    public void testCaseInsensitiveMap() {
        CaseInsensitiveStringMap<String> m1 = new CaseInsensitiveStringMap<>();
        m1.put("a", "a");
        Map<String, String> m2 = new HashMap<>();
        m2.put("a", "a");
        Assert.assertEquals(m2, m1);
        Assert.assertTrue(m1.containsKey("A"));
        Assert.assertFalse(m1.containsValue("A"));
    }

    @Test
    public void testCaseInsensitiveSet() {
        CaseInsensitiveStringSet s1 = new CaseInsensitiveStringSet();
        s1.add("a");
        Set<String> s2 = new HashSet<>();
        s2.add("a");
        Assert.assertEquals(s2, s1);
        Assert.assertTrue(s1.contains("A"));
    }
}
