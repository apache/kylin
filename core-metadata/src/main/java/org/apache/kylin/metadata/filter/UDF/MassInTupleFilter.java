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

package org.apache.kylin.metadata.filter.UDF;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.FunctionTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.function.Functions;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class MassInTupleFilter extends FunctionTupleFilter {
    public static final Logger logger = LoggerFactory.getLogger(MassInTupleFilter.class);
    public static MassInValueProviderFactory VALUE_PROVIDER_FACTORY = null;

    private transient MassInValueProvider valueProvider = null;
    private transient TblColRef column;

    private String filterTableName;//key in MetadataManager.extFilterMap
    private String filterTableResourceIdentifier;//HDFS path, or hbase table name depending on FilterTableType
    private Functions.FilterTableType filterTableType;
    private boolean reverse = false;

    public MassInTupleFilter() {
        super(Lists.<TupleFilter> newArrayList(), TupleFilter.FilterOperatorEnum.MASSIN);
    }

    public MassInTupleFilter(MassInTupleFilter filter) {
        super(new ArrayList<TupleFilter>(filter.children), filter.operator);
        this.valueProvider = filter.getValueProvider();
        this.column = filter.getColumn();
        this.filterTableName = filter.getFilterTableName();
        this.filterTableResourceIdentifier = filter.getFilterTableResourceIdentifier();
        this.filterTableType = filter.getFilterTableType();
        this.reverse = filter.isReverse();
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        Preconditions.checkNotNull(tuple);
        Preconditions.checkNotNull(column);

        Object colValue = tuple.getValue(column);

        if (valueProvider == null) {
            valueProvider = VALUE_PROVIDER_FACTORY.getProvider(filterTableType, filterTableResourceIdentifier, column);
        }
        boolean ret = valueProvider.getMassInValues().contains(colValue);
        return reverse ? !ret : ret;
    }

    @Override
    public TupleFilter reverse() {
        try {
            MassInTupleFilter result = (MassInTupleFilter) this.clone();
            result.setReverse(!this.isReverse());
            return result;
        } catch (CloneNotSupportedException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    public Collection<?> getValues() {
        if (valueProvider == null) {
            valueProvider = VALUE_PROVIDER_FACTORY.getProvider(filterTableType, filterTableResourceIdentifier, column);
        }
        return valueProvider.getMassInValues();
    }

    public TblColRef getColumn() {
        return column;
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    @Override
    public void addChild(TupleFilter child) {
        if (child instanceof ColumnTupleFilter) {
            super.addChild(child);
            ColumnTupleFilter columnFilter = (ColumnTupleFilter) child;
            if (this.column != null) {
                throw new IllegalStateException("Duplicate columns! old is " + column.getName() + " and new is " + columnFilter.getColumn().getName());
            }
            this.column = columnFilter.getColumn();

        } else if (child instanceof ConstantTupleFilter) {
            // super.addChild(child) is omitted because the filter table name is useless at storage side, 
            // we'll extract the useful filterTableResourceIdentifier,filterTableType etc and save it at the MassInTupleFilter itself

            if (filterTableName == null) {
                filterTableName = (String) child.getValues().iterator().next();
                ExternalFilterDesc externalFilterDesc = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getExtFilterDesc(filterTableName);
                if (externalFilterDesc == null) {
                    throw new IllegalArgumentException("External filter named " + filterTableName + " is not found");
                }
                filterTableType = externalFilterDesc.getFilterTableType();
                filterTableResourceIdentifier = externalFilterDesc.getFilterResourceIdentifier();
            }
        } else {
            throw new IllegalStateException("MassInTupleFilter only has two children: one ColumnTupleFilter and one ConstantTupleFilter");
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void serialize(IFilterCodeSystem cs, ByteBuffer buffer) {
        BytesUtil.writeUTFString(filterTableName, buffer);
        BytesUtil.writeUTFString(filterTableResourceIdentifier, buffer);
        BytesUtil.writeUTFString(filterTableType.toString(), buffer);
        BytesUtil.writeUTFString(String.valueOf(reverse), buffer);
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        filterTableName = BytesUtil.readUTFString(buffer);
        filterTableResourceIdentifier = BytesUtil.readUTFString(buffer);
        filterTableType = Functions.FilterTableType.valueOf(BytesUtil.readUTFString(buffer));
        reverse = Boolean.parseBoolean(BytesUtil.readUTFString(buffer));
    }

    public static boolean containsMassInTupleFilter(TupleFilter filter) {
        if (filter == null)
            return false;

        if (filter instanceof MassInTupleFilter) {
            return true;
        }

        for (TupleFilter child : filter.getChildren()) {
            if (containsMassInTupleFilter(child))
                return true;
        }
        return false;
    }

    public boolean isReverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new MassInTupleFilter(this);
    }

    public MassInValueProvider getValueProvider() {
        return valueProvider;
    }

    public String getFilterTableName() {
        return filterTableName;
    }

    public String getFilterTableResourceIdentifier() {
        return filterTableResourceIdentifier;
    }

    public Functions.FilterTableType getFilterTableType() {
        return filterTableType;
    }
}
