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

package org.apache.kylin.common.util;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Range;
import org.apache.kylin.guava30.shaded.common.collect.RangeSet;
import org.apache.kylin.guava30.shaded.common.collect.TreeRangeSet;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.type.LogicalType;

// copy from jackson-datatype-guava
public class TreeRangeSetDeserializer extends JsonDeserializer<RangeSet<Comparable<?>>>
        implements ContextualDeserializer {
    private JavaType genericRangeListType;

    @Override // since 2.12
    public LogicalType logicalType() {
        return LogicalType.Collection;
    }

    @Override
    public RangeSet<Comparable<?>> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (genericRangeListType == null) {
            throw new JsonParseException(p,
                    "RangeSetJsonSerializer was not contextualized (no deserialize target type). "
                            + "You need to specify the generic type down to the generic parameter of RangeSet.");
        } else {
            @SuppressWarnings("unchecked")
            final Iterable<Range<Comparable<?>>> ranges = (Iterable<Range<Comparable<?>>>) ctxt
                    .findContextualValueDeserializer(genericRangeListType, null).deserialize(p, ctxt);
            return TreeRangeSet.create(ranges);
        }
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
        final JavaType genericType = ctxt.getContextualType().containedType(0);
        if (genericType == null)
            return this;
        final TreeRangeSetDeserializer deserializer = new TreeRangeSetDeserializer();
        deserializer.genericRangeListType = ctxt.getTypeFactory().constructCollectionType(List.class,
                ctxt.getTypeFactory().constructParametricType(Range.class, genericType));
        return deserializer;
    }
}
