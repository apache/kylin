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

import java.util.Map;
import java.util.Properties;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
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

        // cube don't have correct result for simple query without group by, but let's try to return something makes sense
        olapContext.resetSQLDigest();
        SQLDigest sqlDigest = olapContext.getSQLDigest();
        hackNoGroupByAggregation(sqlDigest);

        // query storage engine
        IStorageQuery storageEngine = StorageFactory.createQuery(olapContext.realization);
        ITupleIterator iterator = storageEngine.search(olapContext.storageContext, sqlDigest, olapContext.returnTupleInfo);
        if (logger.isDebugEnabled()) {
            logger.debug("return TupleIterator...");
        }

        return iterator;
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
                    String str = value.toString();
                    if (compFilter.getColumn().getType().isDateTimeFamily())
                        str = String.valueOf(DateFormat.stringToMillis(str));
                    
                    compFilter.bindVariable(variable, str);
                }

            }
        }
    }

    private void setConnectionProperties() {
        CalciteConnection conn = (CalciteConnection) optiqContext.getQueryProvider();
        Properties connProps = conn.getProperties();

        String propThreshold = connProps.getProperty(OLAPQuery.PROP_SCAN_THRESHOLD);
        int threshold = Integer.valueOf(propThreshold);
        olapContext.storageContext.setThreshold(threshold);
    }

    // Hack no-group-by query for better results
    private void hackNoGroupByAggregation(SQLDigest sqlDigest) {
        if (!sqlDigest.groupbyColumns.isEmpty() || !sqlDigest.metricColumns.isEmpty())
            return;

        // If no group by and metric found, then it's simple query like select ... from ... where ...,
        // But we have no raw data stored, in order to return better results, we hack to output sum of metric column
        logger.info("No group by and aggregation found in this query, will hack some result for better look of output...");

        // If it's select * from ...,
        // We need to retrieve cube to manually add columns into sqlDigest, so that we have full-columns results as output.
        IRealization cube = olapContext.realization;
        boolean isSelectAll = sqlDigest.allColumns.isEmpty() || sqlDigest.allColumns.equals(sqlDigest.filterColumns);
        for (TblColRef col : cube.getAllColumns()) {
            if (col.getTable().equals(sqlDigest.factTable) && (cube.getAllDimensions().contains(col) || isSelectAll)) {
                sqlDigest.allColumns.add(col);
            }
        }

        for (TblColRef col : sqlDigest.allColumns) {
            // For dimension columns, take them as group by columns.
            if (cube.getAllDimensions().contains(col)) {
                sqlDigest.groupbyColumns.add(col);
            }
            // For measure columns, take them as metric columns with aggregation function SUM().
            else {
                ParameterDesc colParameter = new ParameterDesc();
                colParameter.setType("column");
                colParameter.setValue(col.getName());
                FunctionDesc sumFunc = new FunctionDesc();
                sumFunc.setExpression("SUM");
                sumFunc.setParameter(colParameter);

                boolean measureHasSum = false;
                for (MeasureDesc colMeasureDesc : cube.getMeasures()) {
                    if (colMeasureDesc.getFunction().equals(sumFunc)) {
                        measureHasSum = true;
                        break;
                    }
                }
                if (measureHasSum) {
                    sqlDigest.aggregations.add(sumFunc);
                } else {
                    logger.warn("SUM is not defined for measure column " + col + ", output will be meaningless.");
                }

                sqlDigest.metricColumns.add(col);
            }
        }
    }

}
