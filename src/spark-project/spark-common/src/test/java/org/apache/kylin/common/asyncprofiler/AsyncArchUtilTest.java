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

package org.apache.kylin.common.asyncprofiler;

import org.junit.Assert;
import org.junit.Test;

public class AsyncArchUtilTest {

    @Test
    public void testGetProcessor() {
        AsyncArchUtil.ArchType archType = AsyncArchUtil.getProcessor();
        Assert.assertNotNull(archType);
    }

    @Test
    public void testArchType() {
        AsyncArchUtil.ArchType archType = AsyncArchUtil.getProcessor("x86_64");
        Assert.assertEquals(AsyncArchUtil.ArchType.LINUX_X64, archType);

        archType = AsyncArchUtil.getProcessor("aarch64");
        Assert.assertEquals(AsyncArchUtil.ArchType.LINUX_ARM64, archType);
    }
}
