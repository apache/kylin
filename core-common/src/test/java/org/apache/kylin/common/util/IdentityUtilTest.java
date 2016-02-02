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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 */
public class IdentityUtilTest {
    @Test
    public void basicTest() {
        String s1 = new String("hi");
        String s2 = new String("hi");

        List<String> c1 = Lists.newArrayList(s1);
        List<String> c2 = Lists.newArrayList(s2);
        List<String> c3 = Lists.newArrayList(s2);

        Assert.assertFalse(IdentityUtils.collectionReferenceEquals(c1, c2));
        Assert.assertTrue(IdentityUtils.collectionReferenceEquals(c3, c2));
    }
}
