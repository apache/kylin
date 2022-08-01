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
package org.apache.kylin.sdk.datasource.adaptor;

import org.junit.Assert;
import org.junit.Test;

public class AdaptorConfigTest {
    @Test
    public void testEquals() {
        AdaptorConfig conf1 = new AdaptorConfig("a", "b", "c", "d", null);
        AdaptorConfig conf2 = new AdaptorConfig("a", "b", "c", "d", null);
        AdaptorConfig conf3 = new AdaptorConfig("a1", "b1", "c1", "d1", null);

        Assert.assertEquals(conf1, conf2);
        Assert.assertEquals(conf1.hashCode(), conf2.hashCode());
        Assert.assertNotSame(conf1, conf2);
        Assert.assertNotEquals(conf1, conf3);
        Assert.assertNotEquals(conf1.hashCode(), conf3.hashCode());
    }
}
