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

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 10/28/15.
 */
@SuppressWarnings("rawtypes")
public class MultipleDictionaryValueEnumerator implements IDictionaryValueEnumerator {
    private int curDictIndex = 0;
    private Dictionary curDict;
    private int curKey;
    private byte[] curValue = null;
    private List<Dictionary> dictionaryList;

    public MultipleDictionaryValueEnumerator(List<DictionaryInfo> dictionaryInfoList) {
        dictionaryList = Lists.newArrayListWithCapacity(dictionaryInfoList.size());
        for (DictionaryInfo dictInfo : dictionaryInfoList) {
            dictionaryList.add(dictInfo.getDictionaryObject());
        }
        if (!dictionaryList.isEmpty()) {
            curDict = dictionaryList.get(0);
            curKey = curDict.getMinId();
        }
    }

    @Override
    public byte[] current() throws IOException {
        return curValue;
    }

    @Override
    public boolean moveNext() throws IOException {
        while (curDictIndex < dictionaryList.size()) {
            if (curKey <= curDict.getMaxId()) {
                byte[] buffer = new byte[curDict.getSizeOfValue()];
                int size = curDict.getValueBytesFromId(curKey, buffer, 0);
                curValue = Bytes.copy(buffer, 0, size);
                curKey ++;

                return true;
            }

            // move to next dict if exists
            if (++curDictIndex < dictionaryList.size()) {
                curDict = dictionaryList.get(curDictIndex);
                curKey = curDict.getMinId();
            }
        }
        curValue = null;
        return false;
    }

    @Override
    public void close() throws IOException {
    }
}
