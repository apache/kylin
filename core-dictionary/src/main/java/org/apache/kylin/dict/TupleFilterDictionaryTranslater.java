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

import com.google.common.primitives.Primitives;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.IDictionaryAware;
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

import java.util.ListIterator;

/**
 * Created by dongli on 1/7/16.
 */
public class TupleFilterDictionaryTranslater implements ITupleFilterTranslator {
    public static final Logger logger = LoggerFactory.getLogger(TupleFilterDictionaryTranslater.class);

    private IDictionaryAware dictionaryAware;

    public TupleFilterDictionaryTranslater(IDictionaryAware dictionaryAware) {
        this.dictionaryAware = dictionaryAware;
    }

    @Override
    public TupleFilter translate(TupleFilter tupleFilter) {
        TupleFilter translated = tupleFilter;
        if (tupleFilter instanceof CompareTupleFilter) {
            logger.info("Translation to IN clause: " + tupleFilter);
            translated = translateCompareTupleFilter((CompareTupleFilter) tupleFilter);
            logger.info(translated == null ? "Failed, will use Calcite to handle computed comparison." : "Succeed: " + translated);
        } else if (tupleFilter instanceof FunctionTupleFilter) {
            logger.info("Translation to IN clause: " + tupleFilter);
            translated = translateFunctionTupleFilter((FunctionTupleFilter) tupleFilter);
            logger.info(translated == null ? "Failed, will use Calcite to handle computed column." : "Succeed: " + translated);
        } else if (tupleFilter instanceof LogicalTupleFilter) {
            logger.info("Translation to IN clause: " + tupleFilter);
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) tupleFilter.getChildren().listIterator();
            while (childIterator.hasNext()) {
                TupleFilter tempTranslated = translate(childIterator.next());
                if (tempTranslated != null)
                    childIterator.set(tempTranslated);
            }
            logger.info(translated == null ? "Failed, will use Calcite to handle computed column." : "Succeed: " + translated);
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