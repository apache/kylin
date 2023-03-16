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
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.isPrimaryKeyExists;
import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionException;

import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

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
    void testAddPrimaryKey() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        String table = getTestConfig().getMetadataUrl().getIdentifier() + "_epoch";
        jdbcTemplate.execute(String.format(Locale.ROOT, "alter table %s  drop primary key", table));
        Connection conn = jdbcTemplate.getDataSource().getConnection();
        assert !isPrimaryKeyExists(conn, table);
        epochStore = getEpochStore();
        conn = getJdbcTemplate().getDataSource().getConnection();
        assert isPrimaryKeyExists(conn, table);
    }

    @Test
    void testExecuteWithTransaction_RollBack() {

        Epoch mockEpoch = getMockEpoch("test1", "owner1");
        try {
            epochStore.executeWithTransaction(() -> {
                //insert success
                epochStore.insert(mockEpoch);
                Assertions.assertEquals(1, epochStore.list().size());
                Assertions.assertTrue(compareEpoch(mockEpoch, epochStore.list().get(0)));

                if (epochStore.list().size() == 1) {
                    throw new RuntimeException("mock transaction error");
                }
                return null;
            });
            Assertions.fail();
        } catch (RuntimeException e) {
            Assertions.assertEquals("mock transaction error", Throwables.getRootCause(e).getMessage());
            Assertions.assertEquals(0, epochStore.list().size());
        }
    }

    @Test
    void testExecuteWithTransactionTimeout_RollBack() {

        Epoch mockEpoch = getMockEpoch("test1", "owner1");
        // before transaction
        Assertions.assertEquals(0, epochStore.list().size());
        try {
            epochStore.executeWithTransaction(() -> {
                // mock transaction timeout
                await().pollDelay(1100, TimeUnit.MILLISECONDS).until(() -> true);
                epochStore.insertBatch(Lists.newArrayList(mockEpoch));
                return null;
            }, 1);
            Assertions.fail();
        } catch (RuntimeException e) {
            Throwable rootCause = Throwables.getRootCause(e);
            Assertions.assertTrue(rootCause instanceof TransactionException);
            Assertions.assertTrue(rootCause.getMessage().contains("Transaction timed out"));
        }
        // rollback result
        Assertions.assertEquals(0, epochStore.list().size());
    }

    @Test
    void testExecuteWithTransactionTimeoutSuccess() {

        Epoch mockEpoch = getMockEpoch("test1", "owner1");
        Assertions.assertEquals(0, epochStore.list().size());
        epochStore.executeWithTransaction(() -> {
            await().pollDelay(1100, TimeUnit.MILLISECONDS).until(() -> true);
            epochStore.insertBatch(Lists.newArrayList(mockEpoch));
            Assertions.assertEquals(1, epochStore.list().size());
            return null;
        });
        Assertions.assertEquals(1, epochStore.list().size());
    }
}
