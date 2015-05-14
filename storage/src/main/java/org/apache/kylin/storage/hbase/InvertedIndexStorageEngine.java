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

package org.apache.kylin.storage.hbase;

import com.google.common.collect.Range;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.ICachableStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.EndpointTupleIterator;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author yangli9
 */
public class InvertedIndexStorageEngine implements ICachableStorageEngine {

    private static Logger logger = LoggerFactory.getLogger(InvertedIndexStorageEngine.class);

    private IISegment seg;
    private String uuid;
    private EndpointTupleIterator dataIterator;

    public InvertedIndexStorageEngine(IIInstance ii) {
        this.seg = ii.getFirstSegment();
        this.uuid = ii.getUuid();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        String tableName = seg.getStorageLocationIdentifier();

        //HConnection is cached, so need not be closed
        @SuppressWarnings("deprecation")
        HConnection conn = HBaseConnection.get(context.getConnUrl());
        try {
            dataIterator = new EndpointTupleIterator(seg, sqlDigest.filter, sqlDigest.groupbyColumns, new ArrayList<>(sqlDigest.aggregations), context, conn, returnTupleInfo);
            return dataIterator;
        } catch (Throwable e) {
            logger.error("Error when connecting to II htable " + tableName, e);
            throw new IllegalStateException("Error when connecting to II htable " + tableName, e);
        }
    }

    @Override
    public Range<Long> getVolatilePeriod() {
        return dataIterator.getCacheExcludedPeriod();
    }

    @Override
    public String getStorageUUID() {
        return this.uuid;
    }

    @Override
    public boolean isDynamic() {
        return true;
    }
}
