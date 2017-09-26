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

import org.apache.kylin.common.util.Dictionary;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 10/28/15.
 */
@SuppressWarnings("rawtypes")
public class MultipleDictionaryValueEnumerator implements IDictionaryValueEnumerator {
    private int curDictIndex = 0;
    private Dictionary<String> curDict;
    private int curKey;
    private List<Integer> curKeys = Lists.newArrayList();
    private String curValue = null;
    private List<Dictionary<String>> dictionaryList;

    public MultipleDictionaryValueEnumerator(List<DictionaryInfo> dictionaryInfoList) {
        dictionaryList = Lists.newArrayListWithCapacity(dictionaryInfoList.size());
        for (DictionaryInfo dictInfo : dictionaryInfoList) {
            Dictionary<String> dictionary = (Dictionary<String>) dictInfo.getDictionaryObject();
            dictionaryList.add((Dictionary<String>) dictInfo.getDictionaryObject());
            curKeys.add(dictionary.getMinId());
        }
        if (!dictionaryList.isEmpty()) {
            curDict = dictionaryList.get(0);
            curKey = curDict.getMinId();
        }
    }

    @Override
    public String current() throws IOException {
        return curValue;
    }

    @Override
    public boolean moveNext() throws IOException {
        //initialize the minValue as the first available value in the dictionary list
        String minValue = null;
        int curDictIndex = 0;
        for (int i = 0; i < dictionaryList.size(); i++) {
            if (dictionaryList.get(i) == null || curKeys.get(i) >= dictionaryList.get(i).getSize()) {
                continue;
            }
            minValue = dictionaryList.get(i).getValueFromId(curKeys.get(i));
            curDictIndex = i;
            break;
        }
        if (minValue == null) {
            curValue = null;
            return false;
        }
        // multi-merge dictionary forest
        for (int i = 0; i < dictionaryList.size(); i++) {
            if (dictionaryList.get(i) == null || curKeys.get(i) >= dictionaryList.get(i).getSize()) {
                continue;
            }
            if (minValue.compareTo(dictionaryList.get(i).getValueFromId(curKeys.get(i))) > 0) {
                minValue = dictionaryList.get(i).getValueFromId(curKeys.get(i));
                curDictIndex = i;
            }
        }
        curValue = minValue;
        curKeys.set(curDictIndex, curKeys.get(curDictIndex) + 1);
        return true;
    }

    @Override
    public void close() throws IOException {
    }
}
