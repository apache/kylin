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

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;

import com.google.common.collect.Lists;

import io.druid.query.extraction.CascadeExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.LowerExtractionFn;
import io.druid.query.extraction.StrlenExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.extraction.UpperExtractionFn;

public class FunctionConverters {
    private static final ExtractionFn LOWER_EXTRACT = new LowerExtractionFn(null);
    private static final ExtractionFn UPPER_EXTRACT = new UpperExtractionFn(null);

    public static ExtractionFn convert(BuiltInFunctionTupleFilter filter) {
        ExtractionFn childFn = null;
        TupleFilter columnContainer = filter.getColumnContainerFilter();
        if (columnContainer instanceof BuiltInFunctionTupleFilter) {
            childFn = convert((BuiltInFunctionTupleFilter) columnContainer);
            if (childFn == null) {
                return null;
            }
        }

        ExtractionFn parentFn = null;
        switch (filter.getName()) {
        case "EXTRACT_DATE": {
            ConstantTupleFilter constantTupleFilter = filter.getConstantTupleFilter();
            String timeUnit = (String) constantTupleFilter.getValues().iterator().next();
            String format;
            switch (timeUnit) {
            case "YEAR":
                format = "y";
                break;
            case "MONTH":
                format = "M";
                break;
            case "DAY":
                format = "d";
                break;
            default:
                return null;
            }
            parentFn = new TimeFormatExtractionFn(format, null, null, null, true);
            break;
        }
        case "UPPER":
            parentFn = UPPER_EXTRACT;
            break;
        case "LOWER":
            parentFn = LOWER_EXTRACT;
            break;
        case "CHARLENGTH":
            parentFn = StrlenExtractionFn.instance();
            break;
        case "SUBSTRING": // TODO SubstringDimExtractionFn
        default:
            parentFn = null;
        }

        return compose(parentFn, childFn);
    }

    /**
     * Compose f and g, returning an ExtractionFn that computes f(g(x)).
     * @param f function
     * @param g function
     * @return composed function
     */
    private static ExtractionFn compose(final ExtractionFn f, final ExtractionFn g) {
        if (g == null) {
            return f;
        }
        if (f == null) {
            return null;
        }

        final List<ExtractionFn> extractionFns = Lists.newArrayList();

        // Apply g, then f, unwrapping if they are already cascades.
        if (g instanceof CascadeExtractionFn) {
            extractionFns.addAll(Arrays.asList(((CascadeExtractionFn) g).getExtractionFns()));
        } else {
            extractionFns.add(g);
        }

        if (f instanceof CascadeExtractionFn) {
            extractionFns.addAll(Arrays.asList(((CascadeExtractionFn) f).getExtractionFns()));
        } else {
            extractionFns.add(f);
        }

        return new CascadeExtractionFn(extractionFns.toArray(new ExtractionFn[extractionFns.size()]));
    }
}
