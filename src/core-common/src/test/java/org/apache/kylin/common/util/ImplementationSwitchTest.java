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
import java.util.Map;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ImplementationSwitchTest {

    ImplementationSwitch<I> sw;

    public ImplementationSwitchTest() {
        Map<Integer, String> impls = new HashMap<>();
        impls.put(0, "non.exist.class");
        impls.put(1, Impl1.class.getName());
        impls.put(2, Impl2.class.getName());
        sw = new ImplementationSwitch<I>(impls, I.class);
    }

    public interface I {
    }

    public static class Impl1 implements I {
    }

    public static class Impl2 implements I {
    }

    @Test
    public void test() {
        Assert.assertTrue(sw.get(1) instanceof Impl1);
        Assert.assertTrue(sw.get(2) instanceof Impl2);
    }

    @Test
    public void testException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            sw.get(0);
        });
    }
}
