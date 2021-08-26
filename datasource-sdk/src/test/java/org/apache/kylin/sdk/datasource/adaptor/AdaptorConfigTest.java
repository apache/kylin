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

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

public class AdaptorConfigTest extends LocalFileMetadataTestCase {
    @BeforeClass
    public static void setupClass() throws SQLException {
        staticCreateTestMetadata();
    }
    @Test
    public void testEquals() {
        AdaptorConfig conf1 = new AdaptorConfig("jdbc:mysql://fakehost:1433/database", "b", "c", "d");
        AdaptorConfig conf2 = new AdaptorConfig("jdbc:mysql://fakehost:1433/database", "b", "c", "d");
        AdaptorConfig conf3 = new AdaptorConfig("jdbc:mysql://fakehost:1433/database", "b1", "c1", "d1");

        Assert.assertEquals(conf1, conf2);
        Assert.assertEquals(conf1.hashCode(), conf2.hashCode());
        Assert.assertNotSame(conf1, conf2);
        Assert.assertNotEquals(conf1, conf3);
        Assert.assertNotEquals(conf1.hashCode(), conf3.hashCode());
    }
}
