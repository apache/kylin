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

import java.util.ListIterator;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.FunctionTupleFilter;
import org.apache.kylin.metadata.filter.ITupleFilterTranslator;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Primitives;

/**
 * only take effect when the compare filter has function
 */
public class TupleFilterFunctionTranslator implements ITupleFilterTranslator {
    public static final Logger logger = LoggerFactory.getLogger(TupleFilterFunctionTranslator.class);

    private IDictionaryAware dictionaryAware;

    public TupleFilterFunctionTranslator(IDictionaryAware dictionaryAware) {
        this.dictionaryAware = dictionaryAware;
    }

    @Override
    public TupleFilter translate(TupleFilter tupleFilter) {
        TupleFilter translated = null;
        if (tupleFilter instanceof CompareTupleFilter) {
            translated = translateCompareTupleFilter((CompareTupleFilter) tupleFilter);
            if (translated != null) {
                logger.info("Translated {" + tupleFilter + "} to IN clause: {" + translated + "}");
            }
        } else if (tupleFilter instanceof FunctionTupleFilter) {
            translated = translateFunctionTupleFilter((FunctionTupleFilter) tupleFilter);
            if (translated != null) {
                logger.info("Translated {" + tupleFilter + "} to IN clause: {" + translated + "}");
            }
        } else if (tupleFilter instanceof LogicalTupleFilter) {
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) tupleFilter.getChildren().listIterator();
            while (childIterator.hasNext()) {
                TupleFilter tempTranslated = translate(childIterator.next());
                if (tempTranslated != null)
                    childIterator.set(tempTranslated);
            }
        }
        return translated == null ? tupleFilter : translated;
    }

    private TupleFilter translateFunctionTupleFilter(FunctionTupleFilter functionTupleFilter) {
        if (!functionTupleFilter.isValid())
            return null;

        TblColRef columnRef = functionTupleFilter.getColumn();
        Dictionary<?> dict = dictionaryAware.getDictionary(columnRef);
        if (dict == null)
            return null;

        CompareTupleFilter translated = new CompareTupleFilter(FilterOperatorEnum.IN);
        translated.addChild(new ColumnTupleFilter(columnRef));

        try {
            for (int i = dict.getMinId(); i <= dict.getMaxId(); i++) {
                Object dictVal = dict.getValueFromId(i);
                if ((Boolean) functionTupleFilter.invokeFunction(dictVal)) {
                    translated.addChild(new ConstantTupleFilter(dictVal));
                }
            }
        } catch (Exception e) {
            logger.debug(e.getMessage());
            return null;
        }
        return translated;
    }

    @SuppressWarnings("unchecked")
    private TupleFilter translateCompareTupleFilter(CompareTupleFilter compTupleFilter) {
        if (compTupleFilter.getFunction() == null)
            return null;

        FunctionTupleFilter functionTupleFilter = compTupleFilter.getFunction();
        if (!functionTupleFilter.isValid())
            return null;

        TblColRef columnRef = functionTupleFilter.getColumn();
        Dictionary<?> dict = dictionaryAware.getDictionary(columnRef);
        if (dict == null)
            return null;

        CompareTupleFilter translated = new CompareTupleFilter(FilterOperatorEnum.IN);
        translated.addChild(new ColumnTupleFilter(columnRef));

        try {
            for (int i = dict.getMinId(); i <= dict.getMaxId(); i++) {
                Object dictVal = dict.getValueFromId(i);
                Object computedVal = functionTupleFilter.invokeFunction(dictVal);
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
                    translated.addChild(new ConstantTupleFilter(dictVal));
                }
            }
        } catch (Exception e) {
            logger.debug(e.getMessage());
            return null;
        }
        return translated;
    }
}