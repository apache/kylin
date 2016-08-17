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

package org.apache.kylin.storage.hbase.cube.v2;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

class ExpectedSizeIterator implements Iterator<byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(ExpectedSizeIterator.class);

    BlockingQueue<byte[]> queue;

    int expectedSize;
    int current = 0;
    long rpcTimeout;
    long timeout;
    long timeoutTS;
    volatile Throwable coprocException;

    public ExpectedSizeIterator(int expectedSize) {
        this.expectedSize = expectedSize;
        this.queue = new ArrayBlockingQueue<byte[]>(expectedSize);

        Configuration hconf = HBaseConnection.getCurrentHBaseConfiguration();
        this.rpcTimeout = hconf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
        this.timeout = this.rpcTimeout * hconf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
        logger.info("rpc timeout is {} and after multiply retry times become {}", this.rpcTimeout, this.timeout);
        this.timeout = Math.max(this.timeout, 5 * 60000);
        this.timeout *= KylinConfig.getInstanceFromEnv().getCubeVisitTimeoutTimes();

        if (BackdoorToggles.getQueryTimeout() != -1) {
            this.timeout = BackdoorToggles.getQueryTimeout();
        }

        this.timeout *= 1.1; // allow for some delay

        logger.info("Final Timeout for ExpectedSizeIterator is: " + this.timeout);

        this.timeoutTS = System.currentTimeMillis() + this.timeout;
    }

    @Override
    public boolean hasNext() {
        return (current < expectedSize);
    }

    @Override
    public byte[] next() {
        if (current >= expectedSize) {
            throw new IllegalStateException("Won't have more data");
        }
        try {
            current++;
            byte[] ret = null;

            while (ret == null && coprocException == null && timeoutTS > System.currentTimeMillis()) {
                ret = queue.poll(5000, TimeUnit.MILLISECONDS);
            }

            if (coprocException != null) {
                throw new RuntimeException("Error in coprocessor", coprocException);
            } else if (ret == null) {
                throw new RuntimeException("Timeout visiting cube!");
            } else {
                return ret;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Error when waiting queue", e);
        }
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    public void append(byte[] data) {
        try {
            queue.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException("error when waiting queue", e);
        }
    }

    public long getRpcTimeout() {
        return this.rpcTimeout;
    }

    public void notifyCoprocException(Throwable ex) {
        coprocException = ex;
    }
}
