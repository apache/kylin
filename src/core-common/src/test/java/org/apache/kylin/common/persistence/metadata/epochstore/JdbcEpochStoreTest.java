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
package org.apache.kylin.common.persistence.metadata.epochstore;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.base.Throwables;

import lombok.val;

@OverwriteProp(key = "kylin.metadata.url", value = "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=")
public final class JdbcEpochStoreTest extends AbstractEpochStoreTest {

    @BeforeEach
    public void setup() {
        epochStore = getEpochStore();
    }

    @AfterEach
    public void destroy() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    @Test
    public void testExecuteWithTransaction_RollBack() {

        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        try {
            epochStore.executeWithTransaction(() -> {
                epochStore.insert(e1);

                //insert success
                Assert.assertEquals(epochStore.list().size(), 1);
                Assert.assertTrue(compareEpoch(e1, epochStore.list().get(0)));

                if (epochStore.list().size() == 1) {
                    throw new RuntimeException("mock transaction error");
                }

                return null;
            });

            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(Throwables.getRootCause(e).getMessage(), "mock transaction error");
            Assert.assertEquals(epochStore.list().size(), 0);
        }

    }
}
