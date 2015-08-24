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

package org.apache.kylin.storage.hbase.ii.coprocessor.endpoint;

import java.util.Map;

import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.IDictionaryAware;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;

import com.google.common.collect.Maps;

/**
 */
public class ClearTextDictionary implements IDictionaryAware {

    private final TableRecordInfoDigest digest;
    private final Map<TblColRef, Integer> columnIndexMap;

    public ClearTextDictionary(TableRecordInfoDigest digest, CoprocessorRowType coprocessorRowType) {
        this.digest = digest;
        this.columnIndexMap = coprocessorRowType.columnIdxMap;
    }

    public ClearTextDictionary(TableRecordInfo tableRecordInfo) {
        this.digest = tableRecordInfo.getDigest();
        this.columnIndexMap = Maps.newHashMap();
        for (int i = 0; i < tableRecordInfo.getColumns().size(); i++) {
            columnIndexMap.put(tableRecordInfo.getColumns().get(i), i);
        }
    }

    @Override
    public int getColumnLength(TblColRef col) {
        return digest.length(columnIndexMap.get(col));
    }

    @Override
    public Dictionary<?> getDictionary(TblColRef col) {
        return null;
    }
}
