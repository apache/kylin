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

package org.apache.kylin.dict;

import java.util.Collection;
import java.util.ListIterator;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.filter.BuildInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.ITupleFilterTransformer;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;

/**
 * only take effect when the compare filter has function
 */
public class BuildInFunctionTransformer implements ITupleFilterTransformer {
    public static final Logger logger = LoggerFactory.getLogger(BuildInFunctionTransformer.class);

    private IDimensionEncodingMap dimEncMap;

    public BuildInFunctionTransformer(IDimensionEncodingMap dimEncMap) {
        this.dimEncMap = dimEncMap;
    }

    @Override
    public TupleFilter transform(TupleFilter tupleFilter) {
        TupleFilter translated = null;
        if (tupleFilter instanceof CompareTupleFilter) {
            //normal case
            translated = translateCompareTupleFilter((CompareTupleFilter) tupleFilter);
            if (translated != null) {
                logger.info("Translated {" + tupleFilter + "} to IN clause: {" + translated + "}");
            }
        } else if (tupleFilter instanceof BuildInFunctionTupleFilter) {
            //like case
            translated = translateFunctionTupleFilter((BuildInFunctionTupleFilter) tupleFilter);
            if (translated != null) {
                logger.info("Translated {" + tupleFilter + "} to IN clause: {" + translated + "}");
            }
        } else if (tupleFilter instanceof LogicalTupleFilter) {
            @SuppressWarnings("unchecked")
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) tupleFilter.getChildren().listIterator();
            while (childIterator.hasNext()) {
                TupleFilter transformed = transform(childIterator.next());
                if (transformed != null)
                    childIterator.set(transformed);
            }
        }
        return translated == null ? tupleFilter : translated;
    }

    private TupleFilter translateFunctionTupleFilter(BuildInFunctionTupleFilter buildInFunctionTupleFilter) {
        if (!buildInFunctionTupleFilter.isValid())
            return null;

        TblColRef columnRef = buildInFunctionTupleFilter.getColumn();
        Dictionary<?> dict = dimEncMap.getDictionary(columnRef);
        if (dict == null)
            return null;

        CompareTupleFilter translated = new CompareTupleFilter(FilterOperatorEnum.IN);
        translated.addChild(new ColumnTupleFilter(columnRef));

        try {
            for (int i = dict.getMinId(); i <= dict.getMaxId(); i++) {
                Object dictVal = dict.getValueFromId(i);
                if ((Boolean) buildInFunctionTupleFilter.invokeFunction(dictVal)) {
                    translated.addChild(new ConstantTupleFilter(dictVal));
                }
            }
        } catch (Exception e) {
            logger.debug(e.getMessage());
            return null;
        }
        return translated;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private TupleFilter translateCompareTupleFilter(CompareTupleFilter compTupleFilter) {
        if (compTupleFilter.getFunction() == null || (!(compTupleFilter.getFunction() instanceof BuildInFunctionTupleFilter)))
            return null;

        BuildInFunctionTupleFilter buildInFunctionTupleFilter = (BuildInFunctionTupleFilter) compTupleFilter.getFunction();
        
        if (!buildInFunctionTupleFilter.isValid())
            return null;

        TblColRef columnRef = buildInFunctionTupleFilter.getColumn();
        Dictionary<?> dict = dimEncMap.getDictionary(columnRef);
        if (dict == null)
            return null;

        CompareTupleFilter translated = new CompareTupleFilter(FilterOperatorEnum.IN);
        translated.addChild(new ColumnTupleFilter(columnRef));

        try {
            Collection<Object> inValues = Lists.newArrayList();
            for (int i = dict.getMinId(); i <= dict.getMaxId(); i++) {
                Object dictVal = dict.getValueFromId(i);
                Object computedVal = buildInFunctionTupleFilter.invokeFunction(dictVal);
                Class clazz = Primitives.wrap(computedVal.getClass());
                Object targetVal = compTupleFilter.getFirstValue();
                if (Primitives.isWrapperType(clazz))
                    targetVal = clazz.cast(clazz.getDeclaredMethod("valueOf", String.class).invoke(null, compTupleFilter.getFirstValue()));

                int comp = ((Comparable) computedVal).compareTo(targetVal);
                boolean compResult = false;
                switch (compTupleFilter.getOperator()) {
                case EQ:
                    compResult = comp == 0;
                    break;
                case NEQ:
                    compResult = comp != 0;
                    break;
                case LT:
                    compResult = comp < 0;
                    break;
                case LTE:
                    compResult = comp <= 0;
                    break;
                case GT:
                    compResult = comp > 0;
                    break;
                case GTE:
                    compResult = comp >= 0;
                    break;
                case IN:
                    compResult = compTupleFilter.getValues().contains(computedVal.toString());
                    break;
                case NOTIN:
                    compResult = !compTupleFilter.getValues().contains(computedVal.toString());
                    break;
                default:
                    break;
                }
                if (compResult) {
                    inValues.add(dictVal);
                }
            }
            translated.addChild(new ConstantTupleFilter(inValues));
        } catch (Exception e) {
            logger.debug(e.getMessage());
            return null;
        }
        return translated;
    }
}