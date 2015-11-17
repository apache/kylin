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

import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import org.apache.calcite.sql.SqlOperator;
import org.apache.kylin.metadata.filter.util.BuiltInMethod;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

/**
 * Created by dongli on 11/11/15.
 */
public class FunctionTupleFilter extends TupleFilter {
    public static final Logger logger = LoggerFactory.getLogger(FunctionTupleFilter.class);

    private SqlOperator sqlOperator;
    // FIXME Only supports single parameter functions currently
    private TupleFilter columnContainerFilter;
    private int colPosition;
    private Method method;
    private List<Object> methodParams;
    private boolean isValid = false;

    public FunctionTupleFilter(SqlOperator sqlOperator) {
        super(Lists.<TupleFilter> newArrayList(), FilterOperatorEnum.FUNCTION);
        this.methodParams = Lists.newArrayList();
        this.sqlOperator = sqlOperator;

        String opName = sqlOperator.getName().toUpperCase();
        if (BuiltInMethod.MAP.containsKey(opName)) {
            this.method = BuiltInMethod.MAP.get(opName).method;
            isValid = true;
        }
    }

    public SqlOperator getSqlOperator() {
        return sqlOperator;
    }

    public TblColRef getColumn() {
        if (columnContainerFilter == null)
            return null;

        if (columnContainerFilter instanceof ColumnTupleFilter)
            return ((ColumnTupleFilter) columnContainerFilter).getColumn();
        else if (columnContainerFilter instanceof FunctionTupleFilter)
            return ((FunctionTupleFilter) columnContainerFilter).getColumn();

        throw new UnsupportedOperationException("Wrong type TupleFilter in FunctionTupleFilter.");
    }

    public Object invokeFunction(Object input) throws InvocationTargetException, IllegalAccessException {
        if (columnContainerFilter instanceof ColumnTupleFilter)
            methodParams.set(colPosition, input);
        else if (columnContainerFilter instanceof FunctionTupleFilter)
            methodParams.set(colPosition, ((FunctionTupleFilter) columnContainerFilter).invokeFunction(input));
        return method.invoke(null, (Object[]) (methodParams.toArray()));
    }

    public boolean isValid() {
        return isValid && method != null && methodParams.size() == children.size();
    }

    @Override
    public void addChild(TupleFilter child) {
        if (child instanceof ColumnTupleFilter || child instanceof FunctionTupleFilter) {
            columnContainerFilter = child;
            colPosition = methodParams.size();
            methodParams.add(null);
        } else if (child instanceof ConstantTupleFilter) {
            String constVal = child.getValues().iterator().next();
            try {
                Class<?> clazz = Primitives.wrap(method.getParameterTypes()[methodParams.size()]);
                if (!Primitives.isWrapperType(clazz))
                    methodParams.add(constVal);
                else
                    methodParams.add(clazz.cast(clazz.getDeclaredMethod("valueOf", String.class).invoke(null, constVal)));
            } catch (Exception e) {
                logger.debug(e.getMessage());
                isValid = false;
            }
        }
        super.addChild(child);
    }

    @Override
    public boolean isEvaluable() {
        return false;
    }

    @Override
    public boolean evaluate(ITuple tuple) {
        throw new UnsupportedOperationException("Function filter cannot be evaluated immediately");
    }

    @Override
    public Collection<String> getValues() {
        throw new UnsupportedOperationException("Function filter cannot be evaluated immediately");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(sqlOperator.getName());
        sb.append("(");
        for (int i = 0; i < methodParams.size(); i++) {
            if (colPosition == i) {
                sb.append(columnContainerFilter);
            } else {
                sb.append(methodParams.get(i));
            }
            if (i < methodParams.size() - 1)
                sb.append(",");
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public byte[] serialize() {
        throw new UnsupportedOperationException("Method serialize() is not supported for FunctionTupleFilter.");
    }

    @Override
    public void deserialize(byte[] bytes) {
        throw new UnsupportedOperationException("Method deserialize(byte[] bytes) is not supported for FunctionTupleFilter.");
    }
}
