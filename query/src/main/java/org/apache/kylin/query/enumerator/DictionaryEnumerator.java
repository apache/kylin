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

package org.apache.kylin.query.enumerator;

import java.util.Iterator;
import java.util.List;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@Clarification(deprecated = true, msg = "Only for HBase storage")
public class DictionaryEnumerator implements Enumerator<Object[]> {

    private final static Logger logger = LoggerFactory.getLogger(DictionaryEnumerator.class);

    private List<Dictionary<String>> dictList;
    private final Object[] current;
    private final TblColRef dictCol;
    private final int dictColIdx;
    private Iterator<String> currentDict;
    private Iterator<Dictionary<String>> iterator;

    public DictionaryEnumerator(OLAPContext olapContext) {
        Preconditions.checkArgument(olapContext.allColumns.size() == 1, "The query should only relate to one column");

        dictCol = olapContext.allColumns.iterator().next();
        Preconditions.checkArgument(ifColumnHaveDictionary(dictCol, olapContext.realization, false),
                "The column " + dictCol + " should be encoded as dictionary for " + olapContext.realization);

        dictList = getAllDictionaries(dictCol, olapContext.realization);
        current = new Object[olapContext.returnTupleInfo.size()];
        dictColIdx = olapContext.returnTupleInfo.getColumnIndex(dictCol);

        reset();
        logger.info("Will use DictionaryEnumerator to answer query which is only related to column " + dictCol);
    }

    public static boolean ifDictionaryEnumeratorEligible(OLAPContext olapContext) {
        if (olapContext.allColumns.size() != 1) {
            return false;
        }

        TblColRef dictCol = olapContext.allColumns.iterator().next();
        if (!ifColumnHaveDictionary(dictCol, olapContext.realization, true)) {
            return false;
        }
        return true;
    }

    private static boolean ifColumnHaveDictionary(TblColRef col, IRealization realization, boolean enableCheck) {
        if (realization instanceof CubeInstance) {
            final CubeInstance cube = (CubeInstance) realization;
            boolean ifEnabled = !enableCheck || cube.getConfig().isDictionaryEnumeratorEnabled();
            return ifEnabled && cube.getDescriptor().getAllDimsHaveDictionary().contains(col);
        } else if (realization instanceof HybridInstance) {
            final HybridInstance hybridInstance = (HybridInstance) realization;
            for (IRealization entry : hybridInstance.getRealizations()) {
                if (!ifColumnHaveDictionary(col, entry, enableCheck)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static List<Dictionary<String>> getAllDictionaries(TblColRef col, IRealization realization) {
//        Set<Dictionary<String>> result = Sets.newHashSet();
//        if (realization instanceof CubeInstance) {
//            final CubeInstance cube = (CubeInstance) realization;
//            for (CubeSegment segment : cube.getSegments(SegmentStatusEnum.READY)) {
//                result.add(segment.getDictionary(col));
//            }
//        } else if (realization instanceof HybridInstance) {
//            final HybridInstance hybridInstance = (HybridInstance) realization;
//            for (IRealization entry : hybridInstance.getRealizations()) {
//                result.addAll(getAllDictionaries(col, entry));
//            }
//        } else {
//            throw new IllegalStateException("All leaf realizations should be CubeInstance");
//        }
        return Lists.newArrayList();
    }

    @Override
    public boolean moveNext() {
        while (currentDict == null || !currentDict.hasNext()) {
            if (!iterator.hasNext()) {
                return false;
            }
            final Dictionary<String> dict = iterator.next();
            currentDict = dict.enumeratorValues().iterator();
        }

        current[dictColIdx] = Tuple.convertOptiqCellValue(currentDict.next(), dictCol.getDatatype());
        return true;
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public void reset() {
        iterator = dictList.iterator();
    }

    @Override
    public void close() {
    }
}