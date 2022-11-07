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

package org.apache.kylin.metadata.epoch;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.jdbc.core.JdbcTemplate;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnhancedUnitOfWorkTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TransactionExceptedException transactionThrown = TransactionExceptedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        overwriteSystemProp("kylin.env", "dev");
        getTestConfig().setMetadataUrl("test" + System.currentTimeMillis()
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            resourceStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
            return null;
        }, "");
    }

    @After
    public void tearDown() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    @Test
    public void testEpochNull() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertNull(epochManager.getGlobalEpoch());
        thrown.expect(TransactionException.class);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            System.out.println("just for test");
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    @Test
    public void testEpochExpired() throws Exception {
        overwriteSystemProp("kylin.server.leader-race.heart-beat-timeout", "1");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();

        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        TimeUnit.SECONDS.sleep(2);
        thrown.expect(TransactionException.class);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            System.out.println("just for test");
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    @Test
    public void testEpochIdNotMatch() throws Exception {
        KylinConfig config = getTestConfig();
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        val epoch = epochManager.getGlobalEpoch();
        epoch.setLastEpochRenewTime(System.currentTimeMillis());
        val table = config.getMetadataUrl().getIdentifier();
        thrown.expect(TransactionException.class);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/p1/abc", ByteSource.wrap("abc".getBytes(Charset.defaultCharset())), -1);
            return 0;
        }, 0, UnitOfWork.GLOBAL_UNIT);

    }

    @Test
    public void testSetMaintenanceMode() throws Exception {
        KylinConfig config = getTestConfig();
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        epochManager.setMaintenanceMode("MODE1");
        transactionThrown.expectInTransaction(EpochNotMatchException.class);
        transactionThrown.expectMessageInTransaction("System is trying to recover service. Please try again later");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/p1/abc", ByteSource.wrap("abc".getBytes(Charset.defaultCharset())), -1);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    @Test
    @Ignore
    public void testUnsetMaintenanceMode() throws Exception {
        testSetMaintenanceMode();
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.unsetMaintenanceMode("MODE1");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/p1/abc", ByteSource.wrap("abc".getBytes(Charset.defaultCharset())), -1);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT, 1);
        Assert.assertEquals(0, ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getResource("/_global/p1/abc").getMvcc());
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
