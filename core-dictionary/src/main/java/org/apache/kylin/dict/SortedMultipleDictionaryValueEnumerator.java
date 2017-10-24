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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 10/28/15.
 */
@SuppressWarnings("rawtypes")
public class SortedMultipleDictionaryValueEnumerator implements IDictionaryValueEnumerator {
    private List<Integer> curKeys = Lists.newArrayList();
    private String curValue = null;
    private List<Dictionary<String>> dictionaryList;
    private IForestTreeDictionaryBuilder forestTreeDictionaryBuilder;

    public SortedMultipleDictionaryValueEnumerator(List<DictionaryInfo> dictionaryInfoList,
            IForestTreeDictionaryBuilder forestTreeDictionaryBuilder) {
        dictionaryList = Lists.newArrayListWithCapacity(dictionaryInfoList.size());
        this.forestTreeDictionaryBuilder = forestTreeDictionaryBuilder;
        for (DictionaryInfo dictInfo : dictionaryInfoList) {
            Dictionary<String> dictionary = (Dictionary<String>) dictInfo.getDictionaryObject();
            dictionaryList.add((Dictionary<String>) dictInfo.getDictionaryObject());
            curKeys.add(dictionary.getMinId());
        }
    }

    @Override
    public String current() throws IOException {
        return curValue;
    }

    @Override
    public boolean moveNext() throws IOException {
        String minValue = null;
        byte[] minValueBytes = null;
        int curDictIndex = 0;

        // multi-merge dictionary forest
        for (int i = 0; i < dictionaryList.size(); i++) {
            Dictionary<String> dict = dictionaryList.get(i);
            if (dict == null)
                continue;

            int curKey = curKeys.get(i);
            if (curKey > dict.getMaxId())
                continue;

            String curValue = dict.getValueFromId(curKey);
            byte[] curValueBytes = forestTreeDictionaryBuilder.convertValue(curValue);
            if (minValue == null) {
                minValue = curValue;
                minValueBytes = curValueBytes;
                curDictIndex = i;
            } else {
                if (new ByteArray(minValueBytes).compareTo(new ByteArray(curValueBytes)) > 0) {
                    minValue = curValue;
                    minValueBytes = curValueBytes;
                    curDictIndex = i;
                }
            }
        }

        if (minValue == null) {
            curValue = null;
            return false;
        }

        curValue = minValue;
        curKeys.set(curDictIndex, curKeys.get(curDictIndex) + 1);
        return true;
    }

    @Override
    public void close() throws IOException {
    }
}
