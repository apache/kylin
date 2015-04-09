/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.invertedindex.util;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.TblColRef;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Created by qianzhou on 4/9/15.
 */
public final class IIDictionaryBuilder {

    private IIDictionaryBuilder(){}

    public static Dictionary<?>[] buildDictionary(List<List<String>> table, IIDesc desc) {
        HashMultimap<TblColRef, String> valueMap = HashMultimap.create();
        final List<TblColRef> allColumns = desc.listAllColumns();
        for (List<String> row : table) {
            for (int i = 0; i < row.size(); i++) {
                String cell = row.get(i);
                if (!desc.isMetricsCol(i)) {
                    valueMap.put(allColumns.get(i), cell);
                }
            }
        }

        Dictionary<?>[] result = new Dictionary<?>[allColumns.size()];
        for (TblColRef tblColRef : valueMap.keySet()) {
            final Collection<byte[]> bytes = Collections2.transform(valueMap.get(tblColRef), new Function<String, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(String input) {
                    return input == null ? null : input.getBytes();
                }
            });
            final Dictionary<?> dict = DictionaryGenerator.buildDictionaryFromValueList(tblColRef.getType(), bytes);
            result[desc.findColumn(tblColRef)] = dict;
        }
        return result;
    }
}
