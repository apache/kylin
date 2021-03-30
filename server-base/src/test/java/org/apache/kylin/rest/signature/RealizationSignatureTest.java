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

package org.apache.kylin.rest.signature;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RealizationSignatureTest extends LocalFileMetadataTestCase {

    private KylinConfig kylinConfig;

    @Before
    public void setUp() {
        this.createTestMetadata();
        kylinConfig = getTestConfig();
    }
    @Test
    public void testEquals() {
        RealizationSignature rs = RealizationSignature.CubeSignature.getCubeSignature(kylinConfig, "ssb_cube1");
        RealizationSignature rs1 = RealizationSignature.CubeSignature.getCubeSignature(kylinConfig, "ssb_cube1");
        RealizationSignature rs2 = RealizationSignature.CubeSignature.getCubeSignature(kylinConfig, "ssb_cube2");
        Assert.assertTrue(rs.equals(rs));
        Assert.assertTrue(rs.equals(rs1));
        Assert.assertFalse(rs.equals(null));
        Assert.assertFalse(rs.equals(rs2));
    }
}
