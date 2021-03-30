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

package org.apache.kylin.source.hive;

import org.junit.Assert;
import org.junit.Test;

public class BeelineHIveClientTest {
    @Test
    public void testBasics() {
        String dataType = "varchar";
        String precision = "60";
        String scale = null;
        dataType = BeelineHiveClient.considerDataTypePrecision(dataType, precision, scale);
        Assert.assertEquals("varchar(60)", dataType);

        dataType = "char";
        precision = "50";
        scale = null;
        dataType = BeelineHiveClient.considerDataTypePrecision(dataType, precision, scale);
        Assert.assertEquals("char(50)", dataType);

        dataType = "decimal";
        precision = "8";
        scale = "4";
        dataType = BeelineHiveClient.considerDataTypePrecision(dataType, precision, scale);
        Assert.assertEquals("decimal(8,4)", dataType);

        dataType = "numeric";
        precision = "7";
        scale = "3";
        dataType = BeelineHiveClient.considerDataTypePrecision(dataType, precision, scale);
        Assert.assertEquals("numeric(7,3)", dataType);
    }
}
