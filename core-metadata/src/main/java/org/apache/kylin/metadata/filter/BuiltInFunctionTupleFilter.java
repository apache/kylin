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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.function.BuiltInMethod;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.primitives.Primitives;

public class BuiltInFunctionTupleFilter extends FunctionTupleFilter {
    public static final Logger logger = LoggerFactory.getLogger(BuiltInFunctionTupleFilter.class);

    protected String name;
    // FIXME Only supports single parameter functions currently
    protected TupleFilter columnContainerFilter;//might be a ColumnTupleFilter(simple case) or FunctionTupleFilter(complex case like substr(lower()))
    protected ConstantTupleFilter constantTupleFilter;
    protected int colPosition;
    protected int constantPosition;
    protected Method method;
    protected List<Serializable> methodParams;
    protected boolean isValidFunc = false;
    protected boolean isReversed = false;

    final static Map<String, String> converters = Maps.newHashMap();
    static {
        converters.put("||", "CONCAT");
    }

    public BuiltInFunctionTupleFilter(String name) {
        this(name, null);
    }

    public BuiltInFunctionTupleFilter(String name, FilterOperatorEnum filterOperatorEnum) {
        super(Lists.<TupleFilter> newArrayList(),
                filterOperatorEnum == null ? FilterOperatorEnum.FUNCTION : filterOperatorEnum);
        this.methodParams = Lists.newArrayList();

        if (name != null) {
            this.name = name.toUpperCase(Locale.ROOT);
            initMethod();
        }
    }

    public String getName() {
        return name;
    }

    public ConstantTupleFilter getConstantTupleFilter() {
        return constantTupleFilter;
    }

    public TupleFilter getColumnContainerFilter() {
        return columnContainerFilter;
    }

    public TblColRef getColumn() {
        if (columnContainerFilter == null)
            return null;

        if (columnContainerFilter instanceof ColumnTupleFilter)
            return ((ColumnTupleFilter) columnContainerFilter).getColumn();
        else if (columnContainerFilter instanceof BuiltInFunctionTupleFilter)
            return ((BuiltInFunctionTupleFilter) columnContainerFilter).getColumn();

        throw new UnsupportedOperationException("Wrong type TupleFilter in FunctionTupleFilter.");
    }

    public Object invokeFunction(Object input) throws InvocationTargetException, IllegalAccessException {
        if (columnContainerFilter instanceof ColumnTupleFilter)
            methodParams.set(colPosition, (Serializable) input);
        else if (columnContainerFilter instanceof BuiltInFunctionTupleFilter)
            methodParams.set(colPosition,
                    (Serializable) ((BuiltInFunctionTupleFilter) columnContainerFilter).invokeFunction(input));
        return method.invoke(null, (Object[]) (methodParams.toArray()));
    }

    public boolean isValid() {
        return isValidFunc && method != null && methodParams.size() == children.size();
    }

    @Override
    public TupleFilter reverse() {
        isReversed = !isReversed;
        return this;
    }

    public boolean isReversed() {
        return isReversed;
    }

    public void setReversed(boolean reversed) {
        this.isReversed = reversed;
    }

    @Override
    public void addChild(TupleFilter child) {
        if (child instanceof ColumnTupleFilter || child instanceof BuiltInFunctionTupleFilter) {
            columnContainerFilter = child;
            colPosition = methodParams.size();
            methodParams.add(null);
        } else if (child instanceof ConstantTupleFilter) {
            this.constantTupleFilter = (ConstantTupleFilter) child;
            Serializable constVal = (Serializable) child.getValues().iterator().next();
            try {
                constantPosition = methodParams.size();
                Class<?> clazz = Primitives.wrap(method.getParameterTypes()[methodParams.size()]);
                if (!Primitives.isWrapperType(clazz))
                    methodParams.add(constVal);
                else
                    methodParams.add((Serializable) clazz
                            .cast(clazz.getDeclaredMethod("valueOf", String.class).invoke(null, constVal)));
            } catch (Exception e) {
                logger.warn("Reflection failed for methodParams. ", e);
                isValidFunc = false;
            }
        }
        super.addChild(child);
    }

    @Override
    public boolean isEvaluable() {
        return false;
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        throw new UnsupportedOperationException("Function filter cannot be evaluated immediately");
    }

    @Override
    public Collection<?> getValues() {
        throw new UnsupportedOperationException("Function filter cannot be evaluated immediately");
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        BytesUtil.writeUTFString(name, buffer);
        buffer.put((byte) (isReversed ? 1 : 0));
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        this.name = BytesUtil.readUTFString(buffer);
        this.isReversed = buffer.get() != 0;
        this.initMethod();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (isReversed)
            sb.append("NOT ");
        sb.append(name);
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

    protected void initMethod() {
        String operator = BuiltInMethod.MAP.containsKey(name) ? name : converters.get(name);
        if (operator != null) {
            this.method = BuiltInMethod.MAP.get(operator).method;
            isValidFunc = true;
        }
    }
}