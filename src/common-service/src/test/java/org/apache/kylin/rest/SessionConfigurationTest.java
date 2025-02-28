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
package org.apache.kylin.rest;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.tool.util.MetadataUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.session.SessionProperties;
import org.springframework.boot.autoconfigure.session.StoreType;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
@MetadataInfo(onlyProps = true)
class SessionConfigurationTest {

    @InjectMocks
    SessionConfig configuration = Mockito.spy(new SessionConfig());

    @Mock
    SessionProperties sessionProperties;

    DataSource dataSource;

    @BeforeEach
    public void setup() throws Exception {
        dataSource = Mockito.spy(MetadataUtil.getDataSource(getTestConfig()));
        ReflectionTestUtils.setField(configuration, "dataSource", dataSource);
    }

    @Test
    @OverwriteProp(key = "kylin.metadata.url", value = "haconfigurationtest@jdbc")
    void testInitSessionTablesWithTableNonExists() throws Exception {
        Mockito.when(sessionProperties.getStoreType()).thenReturn(StoreType.JDBC);
        KylinConfig config = getTestConfig();

        String tableName = config.getMetadataUrlPrefix() + "_session_v2";
        Assertions.assertEquals("haconfigurationtest_session_v2", tableName);
        configuration.initSessionTables();
        Assertions.assertTrue(JdbcUtil.isTableExists(dataSource.getConnection(), tableName));
    }

}
