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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http://eli.thegreenplace.net/2011/09/29/an-interesting-tree-serialization-algorithm-from-dwarf
 * 
 * @author xjiang
 * 
 */
public class TupleFilterSerializer {

    private static final Logger logger = LoggerFactory.getLogger(TupleFilterSerializer.class);

    public interface Decorator {
        TupleFilter onSerialize(TupleFilter filter);
    }

    private static final int BUFFER_SIZE = 65536;
    private static final Map<Integer, TupleFilter.FilterOperatorEnum> ID_OP_MAP = new HashMap<Integer, TupleFilter.FilterOperatorEnum>();

    static {
        for (TupleFilter.FilterOperatorEnum op : TupleFilter.FilterOperatorEnum.values()) {
            ID_OP_MAP.put(op.getValue(), op);
        }
    }

    public static byte[] serialize(TupleFilter rootFilter, IFilterCodeSystem<?> cs) {
        return serialize(rootFilter, null, cs);
    }

    public static byte[] serialize(TupleFilter rootFilter, Decorator decorator, IFilterCodeSystem<?> cs) {
        ByteBuffer buffer;
        int bufferSize = BUFFER_SIZE;
        while (true) {
            try {
                buffer = ByteBuffer.allocate(bufferSize);
                internalSerialize(rootFilter, decorator, buffer, cs);
                break;
            } catch (BufferOverflowException e) {
                logger.info("Buffer size {} cannot hold the filter, resizing to 4 times", bufferSize);
                bufferSize *= 4;
            }
        }
        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    private static void internalSerialize(TupleFilter filter, Decorator decorator, ByteBuffer buffer, IFilterCodeSystem<?> cs) {
        if (decorator != null) { // give decorator a chance to manipulate the output filter
            filter = decorator.onSerialize(filter);
        }

        if (filter == null) {
            return;
        }

        if (filter.hasChildren()) {
            // serialize filter+true
            serializeFilter(1, filter, buffer, cs);
            // serialize children
            for (TupleFilter child : filter.getChildren()) {
                internalSerialize(child, decorator, buffer, cs);
            }
            // serialize none
            serializeFilter(-1, filter, buffer, cs);
        } else {
            // serialize filter+false
            serializeFilter(0, filter, buffer, cs);
        }
    }

    private static void serializeFilter(int flag, TupleFilter filter, ByteBuffer buffer, IFilterCodeSystem<?> cs) {
        if (flag < 0) {
            BytesUtil.writeVInt(-1, buffer);
        } else {
            int opVal = filter.getOperator().getValue();
            BytesUtil.writeVInt(opVal, buffer);
            filter.serialize(cs, buffer);
            BytesUtil.writeVInt(flag, buffer);
        }
    }

    public static TupleFilter deserialize(byte[] bytes, IFilterCodeSystem<?> cs) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        TupleFilter rootFilter = null;
        Stack<TupleFilter> parentStack = new Stack<TupleFilter>();
        while (buffer.hasRemaining()) {
            int opVal = BytesUtil.readVInt(buffer);
            if (opVal < 0) {
                parentStack.pop();
                continue;
            }

            // deserialize filter
            TupleFilter filter = createTupleFilter(opVal);
            filter.deserialize(cs, buffer);

            if (rootFilter == null) {
                // push root to stack
                rootFilter = filter;
                parentStack.push(filter);
                BytesUtil.readVInt(buffer);
                continue;
            }

            // add filter to parent
            TupleFilter parentFilter = parentStack.peek();
            if (parentFilter != null) {
                parentFilter.addChild(filter);
            }

            // push filter to stack or not based on having children or not
            int hasChild = BytesUtil.readVInt(buffer);
            if (hasChild == 1) {
                parentStack.push(filter);
            }
        }
        return rootFilter;
    }

    private static TupleFilter createTupleFilter(int opVal) {
        TupleFilter.FilterOperatorEnum op = ID_OP_MAP.get(opVal);
        if (op == null) {
            throw new IllegalStateException("operator value is " + opVal);
        }
        TupleFilter filter = null;
        switch (op) {
        case AND:
        case OR:
        case NOT:
            filter = new LogicalTupleFilter(op);
            break;
        case EQ:
        case NEQ:
        case LT:
        case LTE:
        case GT:
        case GTE:
        case IN:
        case ISNULL:
        case ISNOTNULL:
            filter = new CompareTupleFilter(op);
            break;
        case EXTRACT:
            filter = new ExtractTupleFilter(op);
            break;
        case CASE:
            filter = new CaseTupleFilter();
            break;
        case COLUMN:
            filter = new ColumnTupleFilter(null);
            break;
        case CONSTANT:
            filter = new ConstantTupleFilter();
            break;
        case DYNAMIC:
            filter = new DynamicTupleFilter(null);
            break;
        case FUNCTION:
            filter = new BuiltInFunctionTupleFilter(null);
            break;
        case UNSUPPORTED:
            filter = new UnsupportedTupleFilter(op);
            break;
        case EVAL_FUNC:
            filter = new EvaluatableFunctionTupleFilter(null);
            break;
        case MASSIN:
            filter = new MassInTupleFilter();
            break;
        default:
            throw new IllegalStateException("Error FilterOperatorEnum: " + op.getValue());
        }

        return filter;
    }
}
