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

package org.apache.kylin.metrics.lib.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.metrics.lib.ActiveReservoirListener;
import org.apache.kylin.metrics.lib.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class InstantReservoir extends AbstractActiveReservoir {

    private static final Logger logger = LoggerFactory.getLogger(InstantReservoir.class);

    public void update(Record record) {
        if (!isReady) {
            logger.info("Current reservoir is not ready for update record");
            return;
        }
        onRecordUpdate(record);
    }

    public int size() {
        return 0;
    }

    private void onRecordUpdate(Record record) {
        boolean ifSucceed = true;
        for (ActiveReservoirListener listener : listeners) {
            if (!notifyListenerOfUpdatedRecord(listener, record)) {
                ifSucceed = false;
                logger.warn(
                        "It fails to notify listener " + listener.toString() + " of updated record " + Arrays.toString(record.getKey()));
            }
        }
        if (!ifSucceed) {
            notifyListenerHAOfUpdatedRecord(record);
        }
    }

    private boolean notifyListenerOfUpdatedRecord(ActiveReservoirListener listener, Record record) {
        List<Record> recordsList = Lists.newArrayList();
        recordsList.add(record);
        return listener.onRecordUpdate(recordsList);
    }

    private boolean notifyListenerHAOfUpdatedRecord(Record record) {
        logger.info("The HA listener " + listenerHA.toString() + " for updated record " + Arrays.toString(record.getKey())
                + " will be started");
        if (!notifyListenerOfUpdatedRecord(listenerHA, record)) {
            logger.error("The HA listener also fails!!!");
            return false;
        }
        return true;
    }

}
