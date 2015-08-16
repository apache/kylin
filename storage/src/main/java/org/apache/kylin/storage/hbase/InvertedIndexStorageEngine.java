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

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.EndpointTupleIterator;

/**
 * @author yangli9
 */
public class InvertedIndexStorageEngine implements IStorageEngine {

    private IISegment seg;

    public InvertedIndexStorageEngine(IIInstance ii) {
        this.seg = ii.getFirstSegment();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest) {
        String tableName = seg.getStorageLocationIdentifier();

        // Connection is cached, so need not be closed
        Connection conn = HBaseConnection.get(context.getConnUrl());
        try {
            return new EndpointTupleIterator(seg, sqlDigest.filter, sqlDigest.groupbyColumns, new ArrayList<>(sqlDigest.aggregations), context, conn);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new IllegalStateException("Error when connecting to II htable " + tableName, e);
        }
    }
}
