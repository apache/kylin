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

package org.apache.kylin.storage.druid.read.filter;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import io.druid.js.JavaScriptConfig;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;

public class MoreDimFilters {
    public static final DimFilter ALWAYS_FALSE = new JavaScriptDimFilter("dummy", "function(x){return false;}", null, new JavaScriptConfig(false));
    public static final DimFilter ALWAYS_TRUE = new JavaScriptDimFilter("dummy", "function(x){return true;}", null, new JavaScriptConfig(false));

    public static String getDimension(DimFilter filter) {
        if (filter instanceof SelectorDimFilter) {
            return ((SelectorDimFilter) filter).getDimension();
        }
        if (filter instanceof BoundDimFilter) {
            return ((BoundDimFilter) filter).getDimension();
        }
        if (filter instanceof InDimFilter) {
            return ((InDimFilter) filter).getDimension();
        }
        return null; // TODO support more filters
    }

    public static DimFilter and(List<DimFilter> children) {
        Preconditions.checkNotNull(children);
        Preconditions.checkArgument(!children.isEmpty());

        List<DimFilter> newChildren = new ArrayList<>();
        for (DimFilter child : children) {
            if (child == ALWAYS_FALSE) {
                return child; // and with false, short circuit
            }
            if (child == ALWAYS_TRUE) {
                continue; // and with true, ignore
            }
            if (child instanceof AndDimFilter) {
                newChildren.addAll(((AndDimFilter) child).getFields());
            } else {
                newChildren.add(child);
            }
        }

        if (newChildren.isEmpty()) {
            return ALWAYS_TRUE; // and with all true
        }
        if (newChildren.size() == 1) {
            return newChildren.get(0);
        }
        return new AndDimFilter(newChildren);
    }

    public static DimFilter or(List<DimFilter> children) {
        Preconditions.checkNotNull(children);
        Preconditions.checkArgument(!children.isEmpty());

        List<DimFilter> newChildren = new ArrayList<>();
        for (DimFilter child : children) {
            if (child == ALWAYS_TRUE) {
                return child; // or with true, short circuit
            }
            if (child == ALWAYS_FALSE) {
                continue; // or with false, ignore
            }
            if (child instanceof OrDimFilter) {
                newChildren.addAll(((OrDimFilter) child).getFields());
            } else {
                newChildren.add(child);
            }
        }

        if (newChildren.isEmpty()) {
            return ALWAYS_FALSE; // or with all false
        }
        if (newChildren.size() == 1) {
            return newChildren.get(0);
        }
        return new OrDimFilter(children);
    }

    public static DimFilter not(DimFilter child) {
        Preconditions.checkNotNull(child);

        if (child == ALWAYS_TRUE) {
            return ALWAYS_FALSE;
        }
        if (child == ALWAYS_FALSE) {
            return ALWAYS_TRUE;
        }
        if (child instanceof NotDimFilter) {
            return ((NotDimFilter) child).getField();
        }
        return new NotDimFilter(child);
    }

    public static int leafCount(DimFilter filter) {
        if (filter instanceof AndDimFilter) {
            int count = 0;
            for (DimFilter child : ((AndDimFilter) filter).getFields()) {
                count += leafCount(child);
            }
            return count;
        }

        if (filter instanceof OrDimFilter) {
            int count = 0;
            for (DimFilter child : ((OrDimFilter) filter).getFields()) {
                count += leafCount(child);
            }
            return count;
        }

        if (filter instanceof NotDimFilter) {
            return leafCount(((NotDimFilter) filter).getField());
        }

        return 1;
    }
}
