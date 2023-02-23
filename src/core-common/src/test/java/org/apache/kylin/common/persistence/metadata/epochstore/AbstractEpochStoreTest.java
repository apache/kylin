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
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

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

        Epoch mockEpoch = getMockEpoch("test1", "owner1");
        //insert one
        epochStore.insert(mockEpoch);

        val epochs = epochStore.list();
        Assertions.assertEquals(1, epochs.size());
        Assertions.assertTrue(compareEpoch(mockEpoch, epochs.get(0)));

        //update owner
        mockEpoch.setCurrentEpochOwner("o2");
        epochStore.update(mockEpoch);

        Assertions.assertEquals(mockEpoch.getCurrentEpochOwner(), epochStore.list().get(0).getCurrentEpochOwner());
    }

    @Test
    public void testExecuteWithTransaction_Success() {

        Epoch mockEpoch = getMockEpoch("test1", "owner1");
        epochStore.executeWithTransaction(() -> {
            epochStore.insert(mockEpoch);
            //insert success
            Assertions.assertEquals(1, epochStore.list().size());
            Assertions.assertTrue(compareEpoch(mockEpoch, epochStore.list().get(0)));
            return null;
        });
    }

    @Test
    public void testBatchUpdate() {

        Epoch e1 = getMockEpoch("test1", "owner1");
        Epoch e2 = getMockEpoch("test2", "owner2");

        epochStore.insert(e1);
        epochStore.insert(e2);

        val batchEpochs = Lists.newArrayList(e1, e2);
        epochStore.updateBatch(batchEpochs);
        batchEpochs.forEach(
                epoch -> Assertions.assertTrue(compareEpoch(epoch, epochStore.getEpoch(epoch.getEpochTarget()))));
    }

    @Test
    public void testBatchUpdateWithError() {

        Epoch e1 = getMockEpoch("test1", "owner1");
        Epoch e2 = getMockEpoch("test2", "owner2");

        epochStore.insert(e1);

        boolean isError = false;
        try {
            epochStore.updateBatch(Lists.newArrayList(e1, e2));
        } catch (Exception e) {
            isError = true;
        }
        if (epochStore instanceof JdbcEpochStore) {
            Assertions.assertTrue(isError);
        }
    }

    @Test
    public void testBatchInsert() {
        Epoch e1 = getMockEpoch("test1", "owner1");
        Epoch e2 = getMockEpoch("test2", "owner2");

        val batchEpochs = Arrays.asList(e1, e2);
        epochStore.insertBatch(batchEpochs);
        batchEpochs.forEach(
                epoch -> Assertions.assertTrue(compareEpoch(epoch, epochStore.getEpoch(epoch.getEpochTarget()))));
    }

    @Test
    public void testIsLeaderNodeWithCurrentEpochOwnerNull() {
        Epoch mockEpoch = getMockEpoch("_global", null);
        epochStore.insert(mockEpoch);
        Assertions.assertFalse(EpochStore.isLeaderNode());
    }

    @Test
    public void testIsLeaderNodeWithServiceInfoNotEqual() {
        Epoch mockEpoch = getMockEpoch("_global", "owner1");
        epochStore.insert(mockEpoch);
        Assertions.assertFalse(EpochStore.isLeaderNode());
    }

    @Test
    public void testIsLeaderNodeWithServiceInfoEqual() {
        Assertions.assertFalse(EpochStore.isLeaderNode());
        Epoch mockEpoch = getMockEpoch("_global", AddressUtil.getLocalInstance() + "|" + Long.MAX_VALUE);
        epochStore.insert(mockEpoch);
        Assertions.assertTrue(EpochStore.isLeaderNode());
    }

    protected Epoch getMockEpoch(String epochTarget, String epochOwner) {
        Epoch epoch = new Epoch();
        epoch.setEpochTarget(epochTarget);
        epoch.setCurrentEpochOwner(epochOwner);
        epoch.setEpochId(1);
        epoch.setLastEpochRenewTime(System.currentTimeMillis());
        return epoch;
    }
}
