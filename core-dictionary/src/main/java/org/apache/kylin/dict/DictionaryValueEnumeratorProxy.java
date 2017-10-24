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

/**
 * @author leontong
 *
 */
@SuppressWarnings("rawtypes")
public class DictionaryValueEnumeratorProxy implements IDictionaryValueEnumerator {
    private final IDictionaryValueEnumerator internal;

    public DictionaryValueEnumeratorProxy(List<DictionaryInfo> dictionaryInfoList, IDictionaryBuilder dictionaryBuilder) {
        if (dictionaryBuilder instanceof IForestTreeDictionaryBuilder) {
            internal = new SortedMultipleDictionaryValueEnumerator(dictionaryInfoList,
                    (IForestTreeDictionaryBuilder) dictionaryBuilder);
        } else {
            internal = new MultipleDictionaryValueEnumerator(dictionaryInfoList);
        }
    }

    @Override
    public String current() throws IOException {
        return internal.current();
    }

    @Override
    public boolean moveNext() throws IOException {
        return internal.moveNext();
    }

    @Override
    public void close() throws IOException {
        internal.close();
    }
}
