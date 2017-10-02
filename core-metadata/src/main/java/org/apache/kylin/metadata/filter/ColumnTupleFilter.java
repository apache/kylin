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

package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

/**
 * 
 * @author xjiang
 * 
 */
public class ColumnTupleFilter extends TupleFilter {

    private static final String _QUALIFIED_ = "_QUALIFIED_";
    
    private TblColRef columnRef;
    private Object tupleValue;
    private List<Object> values;

    public ColumnTupleFilter(TblColRef column) {
        super(Collections.<TupleFilter> emptyList(), FilterOperatorEnum.COLUMN);
        this.columnRef = column;
        this.values = new ArrayList<Object>(1);
        this.values.add(null);
    }

    public TblColRef getColumn() {
        return columnRef;
    }

    public void setColumn(TblColRef col) {
        this.columnRef = col;
    }

    @Override
    public void addChild(TupleFilter child) {
        throw new UnsupportedOperationException("This is " + this + " and child is " + child);
    }

    @Override
    public String toString() {
        return "" + columnRef;
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        this.tupleValue = tuple.getValue(columnRef);
        return true;
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    @Override
    public Collection<?> getValues() {
        this.values.set(0, this.tupleValue);
        return this.values;
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        TableRef tableRef = columnRef.getTableRef();
        
        if (tableRef == null) {
            // un-qualified column
            String table = columnRef.getTable();
            BytesUtil.writeUTFString(table, buffer);

            String columnId = columnRef.getColumnDesc().getId();
            BytesUtil.writeUTFString(columnId, buffer);

            String columnName = columnRef.getName();
            BytesUtil.writeUTFString(columnName, buffer);

            String dataType = columnRef.getDatatype();
            BytesUtil.writeUTFString(dataType, buffer);
        } else {
            // qualified column (from model)
            BytesUtil.writeUTFString(_QUALIFIED_, buffer);
            
            String model = tableRef.getModel().getName();
            BytesUtil.writeUTFString(model, buffer);
            
            String alias = tableRef.getAlias();
            BytesUtil.writeUTFString(alias, buffer);
            
            String col = columnRef.getName();
            BytesUtil.writeUTFString(col, buffer);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {

        String tableName = BytesUtil.readUTFString(buffer);
        
        if (_QUALIFIED_.equals(tableName)) {
            // qualified column (from model)
            String model = BytesUtil.readUTFString(buffer);
            String alias = BytesUtil.readUTFString(buffer);
            String col = BytesUtil.readUTFString(buffer);
            
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            DataModelDesc modelDesc = DataModelManager.getInstance(config).getDataModelDesc(model);
            this.columnRef = modelDesc.findColumn(alias, col);
            
        } else {
            // un-qualified column
            TableDesc tableDesc = null;

            if (tableName != null) {
                tableDesc = new TableDesc();
                tableDesc.setName(tableName);
            }

            ColumnDesc column = new ColumnDesc();
            column.setId(BytesUtil.readUTFString(buffer));
            column.setName(BytesUtil.readUTFString(buffer));
            column.setDatatype(BytesUtil.readUTFString(buffer));
            column.init(tableDesc);

            this.columnRef = column.getRef();
        }
    }
}
