/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.query.enumerator;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.eigenbase.reltype.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.cube.model.MeasureDesc;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.storage.IStorageEngine;
import com.kylinolap.storage.StorageEngineFactory;
import com.kylinolap.storage.filter.CompareTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.tuple.ITuple;
import com.kylinolap.storage.tuple.ITupleIterator;

/**
 * @author xjiang
 */
public class CubeEnumerator implements Enumerator<Object[]> {

    private final static Logger logger = LoggerFactory.getLogger(CubeEnumerator.class);

    private final OLAPContext olapContext;
    private final DataContext optiqContext;
    private final Object[] current;
    private ITupleIterator cursor;
    private int[] fieldIndexes;

    public CubeEnumerator(OLAPContext olapContext, DataContext optiqContext) {
        this.olapContext = olapContext;
        this.optiqContext = optiqContext;
        this.current = new Object[olapContext.olapRowType.getFieldCount()];
        this.cursor = null;
        this.fieldIndexes = null;
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

    @Override
    public void reset() {
        close();
        cursor = queryStorage();
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
        }
    }

    private Object[] convertCurrentRow(ITuple tuple) {

        // build field index map
        if (this.fieldIndexes == null) {
            List<String> fields = tuple.getAllFields();
            int size = fields.size();
            this.fieldIndexes = new int[size];
            for (int i = 0; i < size; i++) {
                String field = fields.get(i);
                RelDataTypeField relField = olapContext.olapRowType.getField(field, true);
                if (relField != null) {
                    fieldIndexes[i] = relField.getIndex();
                } else {
                    fieldIndexes[i] = -1;
                }
            }
        }

        // set field value
        Object[] values = tuple.getAllValues();
        for (int i = 0, n = values.length; i < n; i++) {
            Object value = values[i];
            int index = fieldIndexes[i];
            if (index >= 0) {
                current[index] = value;
            }
        }

        return current;
    }

    private ITupleIterator queryStorage() {
        logger.debug("query storage...");

        // set connection properties
        setConnectionProperties();

        // bind dynamic variables
        bindVariable(olapContext.filter);

        // build dimension & metrics
        Collection<TblColRef> dimensions = new HashSet<TblColRef>();
        Collection<FunctionDesc> metrics = new HashSet<FunctionDesc>();
        buildDimensionsAndMetrics(dimensions, metrics);

        // query storage engine
        IStorageEngine storageEngine = StorageEngineFactory.getStorageEngine(olapContext.cubeInstance);
        ITupleIterator iterator = storageEngine.search(dimensions, olapContext.filter, olapContext.groupByColumns, metrics, olapContext.storageContext);
        if (logger.isDebugEnabled()) {
            logger.debug("return TupleIterator...");
        }

        this.fieldIndexes = null;
        return iterator;
    }

    private void buildDimensionsAndMetrics(Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics) {

        for (FunctionDesc func : olapContext.aggregations) {
            if (!func.isAppliedOnDimension()) {
                metrics.add(func);
            }
        }

        if (olapContext.isSimpleQuery()) {
            // In order to prevent coprocessor from doing the real aggregating,
            // All dimensions are injected
            for (DimensionDesc dim : olapContext.cubeDesc.getDimensions()) {
                for (TblColRef col : dim.getColumnRefs()) {
                    dimensions.add(col);
                }
            }
            // select sth from fact table
            for (MeasureDesc measure : olapContext.cubeDesc.getMeasures()) {
                FunctionDesc func = measure.getFunction();
                if (func.isSum()) {
                    // the rewritten name for sum(metric) is metric itself
                    metrics.add(func);
                }
            }
            olapContext.storageContext.markAvoidAggregation();
        } else {
            for (TblColRef column : olapContext.allColumns) {
                // skip measure columns
                if (olapContext.metricsColumns.contains(column)) {
                    continue;
                }
                dimensions.add(column);
            }
        }
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
            for (Map.Entry<String, String> entry : compFilter.getVariables().entrySet()) {
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
