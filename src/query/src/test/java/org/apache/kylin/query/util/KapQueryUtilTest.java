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

package org.apache.kylin.query.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.apache.kylin.common.KylinConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KapQueryUtilTest {

    public static final String SQL = "select * from table1";

    @Test
    public void testMaxResultRowsEnabled() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);
            when(kylinConfig.getMaxResultRows()).thenReturn(15);
            when(kylinConfig.getForceLimit()).thenReturn(14);
            String result = KapQueryUtil.normalMassageSql(kylinConfig, SQL, 16, 0);
            assertEquals("select * from table1" + "\n" + "LIMIT 15", result);
        }
    }

    @Test
    public void testCompareMaxResultRowsAndLimit() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);
            when(kylinConfig.getMaxResultRows()).thenReturn(15);
            when(kylinConfig.getForceLimit()).thenReturn(14);
            String result = KapQueryUtil.normalMassageSql(kylinConfig, SQL, 13, 0);
            assertEquals("select * from table1" + "\n" + "LIMIT 13", result);
        }
    }
}
