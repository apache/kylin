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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.Arrays;
import java.util.Objects;

import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.common.persistence.metadata.JdbcEpochStore;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import lombok.val;

@MetadataInfo(onlyProps = true)
public abstract class AbstractEpochStoreTest {

    EpochStore epochStore;

    EpochStore getEpochStore() {
        try {
            return EpochStore.getEpochStore(getTestConfig());
        } catch (Exception e) {
            throw new RuntimeException("cannnot init epoch store!");
        }
    }

    boolean compareEpoch(Epoch a, Epoch b) {
        return Objects.equals(a.getCurrentEpochOwner(), b.getCurrentEpochOwner())
                && Objects.equals(a.getEpochTarget(), b.getEpochTarget())
                && Objects.equals(a.getEpochId(), b.getEpochId())
                && Objects.equals(a.getLastEpochRenewTime(), b.getLastEpochRenewTime());
    }

    @Test
    public void testInsertAndUpdate() {

        Epoch newSaveEpoch = new Epoch();
        newSaveEpoch.setEpochTarget("test1");
        newSaveEpoch.setCurrentEpochOwner("owner1");
        newSaveEpoch.setEpochId(1);
        newSaveEpoch.setLastEpochRenewTime(System.currentTimeMillis());

        //insert one
        epochStore.insert(newSaveEpoch);
        val epochs = epochStore.list();
        Assert.assertEquals(epochs.size(), 1);

        Assert.assertTrue(compareEpoch(newSaveEpoch, epochs.get(0)));

        //update owner
        newSaveEpoch.setCurrentEpochOwner("o2");
        epochStore.update(newSaveEpoch);

        Assert.assertEquals(newSaveEpoch.getCurrentEpochOwner(), epochStore.list().get(0).getCurrentEpochOwner());

    }

    @Test
    public void testExecuteWithTransaction_Success() {

        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        epochStore.executeWithTransaction(() -> {
            epochStore.insert(e1);

            //insert success
            Assert.assertEquals(epochStore.list().size(), 1);
            Assert.assertTrue(compareEpoch(e1, epochStore.list().get(0)));

            return null;
        });

    }

    @Test
    public void testBatchUpdate() {
        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        epochStore.insert(e1);

        Epoch e2 = new Epoch();
        e2.setEpochTarget("test2");
        e2.setCurrentEpochOwner("owner2");
        e2.setEpochId(1);
        e2.setLastEpochRenewTime(System.currentTimeMillis());
        epochStore.insert(e2);

        val batchEpochs = Arrays.asList(e1, e2);

        epochStore.updateBatch(batchEpochs);

        batchEpochs.forEach(epoch -> {
            Assert.assertTrue(compareEpoch(epoch, epochStore.getEpoch(epoch.getEpochTarget())));
        });

    }

    @Test
    public void testBatchUpdateWithError() {
        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        epochStore.insert(e1);

        Epoch e2 = new Epoch();
        e2.setEpochTarget("test2");
        e2.setCurrentEpochOwner("owner2");
        e2.setEpochId(1);
        e2.setLastEpochRenewTime(System.currentTimeMillis());

        val batchEpochs = Arrays.asList(e1, e2);
        boolean isError = false;
        try {
            epochStore.updateBatch(batchEpochs);
        } catch (Exception e) {
            isError = true;
        }
        if (epochStore instanceof JdbcEpochStore) {
            Assert.assertTrue(isError);
        }
    }

    @Test
    public void testBatchInsert() {
        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        Epoch e2 = new Epoch();
        e2.setEpochTarget("test2");
        e2.setCurrentEpochOwner("owner2");
        e2.setEpochId(1);
        e2.setLastEpochRenewTime(System.currentTimeMillis());

        val batchEpochs = Arrays.asList(e1, e2);

        epochStore.insertBatch(batchEpochs);

        batchEpochs.forEach(epoch -> {
            Assert.assertTrue(compareEpoch(epoch, epochStore.getEpoch(epoch.getEpochTarget())));
        });

    }
}
