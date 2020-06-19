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

package org.apache.kylin.metrics.lib.impl.hive;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.impl.InstantReservoir;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import com.google.common.collect.Lists;

@PrepareForTest({ HiveReservoirReporter.HiveReservoirListener.class })
public class HiveReservoirReporterTest {

    @Rule
    public PowerMockRule rule = new PowerMockRule();

    private HiveReservoirReporter hiveReporter;
    private ActiveReservoir reservoir;

    @Before
    public void setUp() throws Exception {
        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/localmeta");

        HiveProducer hiveProducer = PowerMockito.mock(HiveProducer.class);
        PowerMockito.whenNew(HiveProducer.class).withAnyArguments().thenReturn(hiveProducer);

        reservoir = new InstantReservoir();
        reservoir.start();
        hiveReporter = HiveReservoirReporter.forRegistry(reservoir).build();
    }

    @After
    public void after() throws Exception {
        System.clearProperty(KylinConfig.KYLIN_CONF);
    }

    @Test
    public void testUpdate() throws Exception {
        String metricsType = "TEST";
        Record record = new RecordEvent(metricsType);
        reservoir.update(record);
        assertEquals(0, hiveReporter.getListener().getNRecord());

        hiveReporter.start();
        reservoir.update(record);
        reservoir.update(record);
        assertEquals(2, hiveReporter.getListener().getNRecord());

        hiveReporter.stop();
        reservoir.update(record);
        assertEquals(2, hiveReporter.getListener().getNRecord());

        hiveReporter.start();
        reservoir.update(record);
        PowerMockito.doThrow(new Exception()).when(hiveReporter.getListener().getProducer(metricsType))
                .send(Lists.newArrayList(record));
        reservoir.update(record);
        assertEquals(3, hiveReporter.getListener().getNRecord());
        assertEquals(1, hiveReporter.getListener().getNRecordSkip());
    }
}
