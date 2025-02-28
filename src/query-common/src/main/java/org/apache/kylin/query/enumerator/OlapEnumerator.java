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

package org.apache.kylin.query.enumerator;

import java.util.Arrays;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OlapEnumerator implements Enumerator<Object[]> {

    private static final Logger logger = LoggerFactory.getLogger(OlapEnumerator.class);

    private final OlapContext olapContext;
    private final DataContext optiqContext;
    private Object[] current;
    private ITupleIterator cursor;

    public OlapEnumerator(OlapContext olapContext, DataContext optiqContext) {
        this.olapContext = olapContext;
        this.optiqContext = optiqContext;
        this.cursor = null;
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            if (cursor == null) {
                cursor = queryStorage();
            }

            if (!cursor.hasNext()) {
                return false;
            }

            ITuple tuple = cursor.next();
            if (tuple == null) {
                return false;
            }
            convertCurrentRow(tuple);
            return true;
        } catch (Exception e) {
            try {
                if (cursor != null) {
                    cursor.close();
                }
            } catch (Exception ee) {
                logger.info("Error when closing cursor, ignore it", ee);
            }
            throw e;
        }
    }

    private void convertCurrentRow(ITuple tuple) {
        // give calcite a new array every time, see details in KYLIN-2134
        Object[] values = tuple.getAllValues();
        current = Arrays.copyOf(values, values.length);
    }

    @Override
    public void reset() {
        close();
        cursor = queryStorage();
    }

    @Override
    public void close() {
        if (cursor != null)
            cursor.close();
    }

    private ITupleIterator queryStorage() {
        logger.debug("query storage...");
        // bind dynamic variables and update filter info in OlapContext
        SQLDigest sqlDigest = olapContext.getSQLDigest();

        // query storage engine
        IStorageQuery storageEngine = StorageFactory.createQuery(olapContext.getRealization());
        ITupleIterator iterator = storageEngine.search(olapContext.getStorageContext(), sqlDigest,
                olapContext.getReturnTupleInfo());
        if (logger.isDebugEnabled()) {
            logger.debug("return TupleIterator...");
        }

        return iterator;
    }

}
