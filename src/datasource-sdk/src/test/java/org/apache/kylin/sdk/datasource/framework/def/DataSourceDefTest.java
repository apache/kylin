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

import java.sql.Types;
import java.util.List;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataSourceDefTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        DataSourceDef defaultDef = provider.getDefault();

        Assert.assertNotNull(defaultDef);
        Assert.assertEquals("default", defaultDef.getId());

        // functions
        List<String> funcDefU = defaultDef.getFuncDefsByName("NULLIF");
        List<String> funcDefL = defaultDef.getFuncDefsByName("nullif");
        Assert.assertEquals(funcDefL, funcDefU);
        Assert.assertFalse(funcDefU.isEmpty());
        Assert.assertNotNull(defaultDef.getFuncDefSqlNode(funcDefU.get(0)));

        // types
        List<TypeDef> typeDefU = defaultDef.getTypeDefsByName("STRING");
        List<TypeDef> typeDefL = defaultDef.getTypeDefsByName("string");
        Assert.assertEquals(typeDefU, typeDefL);
        Assert.assertFalse(typeDefL.isEmpty());
        Assert.assertNotNull(defaultDef.getTypeDef(typeDefL.get(0).getId()));

        // properties
        Assert.assertNotNull(defaultDef.getPropertyValue("sql.default-converted-enabled", null));
        Assert.assertNull(defaultDef.getPropertyValue("invalid-key", null));

        // dataTypeMappings
        DataSourceDef testingDsDef = provider.getById("testing");

        Assert.assertEquals(Types.VARCHAR, (int) testingDsDef.getDataTypeValue("CHARACTER VARYING"));
        Assert.assertEquals(Types.DOUBLE, (int) testingDsDef.getDataTypeValue("DOUBLE PRECISION"));
        Assert.assertEquals(Types.DOUBLE, (int) testingDsDef.getDataTypeValue("double precision"));
    }

    @Test
    public void testOverrideXml() {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        DataSourceDef defaultDef = provider.getDefault();
        Assert.assertEquals("true", defaultDef.getPropertyValue("metadata.enable-cache", null)); //in default.xml is false,but in default.xml.override is true
    }
}
