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

package org.apache.kylin.dict.lookup;

import java.io.IOException;

import org.apache.kylin.common.util.Bytes;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * @author yangli9
 * 
 */
public class LookupBytesTable extends LookupTable<ByteArray> {

    public LookupBytesTable(TableDesc tableDesc, String[] keyColumns, ReadableTable table) throws IOException {
        super(tableDesc, keyColumns, table);
    }

    @Override
    protected ByteArray[] convertRow(String[] cols) {
        ByteArray[] r = new ByteArray[cols.length];
        for (int i = 0; i < cols.length; i++) {
            r[i] = cols[i] == null ? null : new ByteArray(Bytes.toBytes(cols[i]));
        }
        return r;
    }

    @Override
    protected String toString(ByteArray cell) {
        return cell.toString();
    }

}
