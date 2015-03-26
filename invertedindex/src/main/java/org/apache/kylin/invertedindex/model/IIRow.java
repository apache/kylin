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

package org.apache.kylin.invertedindex.model;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.BytesUtil;

/**
 * Created by qianzhou on 3/10/15.
 */
public final class IIRow {

    private final ImmutableBytesWritable key;
    private final ImmutableBytesWritable value;
    private final ImmutableBytesWritable dictionary;

    public IIRow(ImmutableBytesWritable key, ImmutableBytesWritable value, ImmutableBytesWritable dictionary) {
        this.key = key;
        this.value = value;
        this.dictionary = dictionary;
    }

    public IIRow() {
        this(new ImmutableBytesWritable(), new ImmutableBytesWritable(), new ImmutableBytesWritable());
    }

    public ImmutableBytesWritable getKey() {
        return key;
    }

    public ImmutableBytesWritable getValue() {
        return value;
    }

    public ImmutableBytesWritable getDictionary() {
        return dictionary;
    }

    public void updateWith(Cell c) {
        if (BytesUtil.compareBytes(IIDesc.HBASE_QUALIFIER_BYTES, 0, c.getQualifierArray(), c.getQualifierOffset(), IIDesc.HBASE_QUALIFIER_BYTES.length) == 0) {
            this.getKey().set(c.getRowArray(), c.getRowOffset(), c.getRowLength());
            this.getValue().set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
        } else if (BytesUtil.compareBytes(IIDesc.HBASE_DICTIONARY_BYTES, 0, c.getQualifierArray(), c.getQualifierOffset(), IIDesc.HBASE_DICTIONARY_BYTES.length) == 0) {
            this.getDictionary().set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
        }
    }
}
