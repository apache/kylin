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

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.DateConditionModifier;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.hbase.coprocessor.DictCodeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @author xjiang
 */
public class OLAPEnumerator implements Enumerator<Object[]> {

    private final static Logger logger = LoggerFactory.getLogger(OLAPEnumerator.class);

    private final OLAPContext olapContext;
    private final DataContext optiqContext;
    private Object[] current;
    private ITupleIterator cursor;

    public OLAPEnumerator(OLAPContext olapContext, DataContext optiqContext) {
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
    }

    private Object[] convertCurrentRow(ITuple tuple) {
        // make sure the tuple layout is correct
        //assert tuple.getAllFields().equals(olapContext.returnTupleInfo.getAllFields());

        current = tuple.getAllValues();
        return current;
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

        // set connection properties
        setConnectionProperties();

        // bind dynamic variables
        bindVariable(olapContext.filter);

        //modify date condition
        olapContext.filter = modifyDateCondition(olapContext.filter);
        olapContext.resetSQLDigest();

        // query storage engine
        IStorageEngine storageEngine = StorageEngineFactory.getStorageEngine(olapContext.realization);
        ITupleIterator iterator = storageEngine.search(olapContext.storageContext, olapContext.getSQLDigest(), olapContext.returnTupleInfo);
        if (logger.isDebugEnabled()) {
            logger.debug("return TupleIterator...");
        }

        return iterator;
    }

    private TupleFilter modifyDateCondition(TupleFilter filter) {
        DateConditionModifier filterDecorator = new DateConditionModifier(filter);
        byte[] bytes = TupleFilterSerializer.serialize(filter, filterDecorator, DictCodeSystem.INSTANCE);
        return TupleFilterSerializer.deserialize(bytes, DictCodeSystem.INSTANCE);
    }

    private void bindVariable(TupleFilter filter) {
        if (filter == null) {
            return;
        }

        for (TupleFilter childFilter : filter.getChildren()) {
            bindVariable(childFilter);
        }

        if (filter instanceof CompareTupleFilter && optiqContext != null) {
            CompareTupleFilter compFilter = (CompareTupleFilter) filter;
            for (Map.Entry<String, Object> entry : compFilter.getVariables().entrySet()) {
                String variable = entry.getKey();
                Object value = optiqContext.get(variable);
                if (value != null) {
                    compFilter.bindVariable(variable, value.toString());
                }

            }
        }
    }

    private void setConnectionProperties() {
        OptiqConnection conn = (OptiqConnection) optiqContext.getQueryProvider();
        Properties connProps = conn.getProperties();

        String propThreshold = connProps.getProperty(OLAPQuery.PROP_SCAN_THRESHOLD);
        int threshold = Integer.valueOf(propThreshold);
        olapContext.storageContext.setThreshold(threshold);
    }
}
