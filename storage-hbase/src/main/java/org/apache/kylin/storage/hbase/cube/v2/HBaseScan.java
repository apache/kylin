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

package org.apache.kylin.storage.hbase.cube.v2;

import java.util.Arrays;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;

import com.google.common.base.Preconditions;

public class HBaseScan {

    /**
     * every column in scan key is fixed length. for empty values, 0 zero will be populated 
     */
    public static ByteArray exportScanKey(GTRecord rec, byte fill) {

        Preconditions.checkNotNull(rec);

        GTInfo info = rec.getInfo();
        int len = info.getMaxColumnLength(info.getPrimaryKey());
        ByteArray buf = ByteArray.allocate(len);
        int pos = 0;
        for (int i = 0; i < info.getPrimaryKey().trueBitCount(); i++) {
            int c = info.getPrimaryKey().trueBitAt(i);
            int colLength = info.getCodeSystem().maxCodeLength(c);

            if (rec.get(c).array() != null) {
                Preconditions.checkArgument(colLength == rec.get(c).length(), "ColLength :" + colLength + " not equals cols[c] length: " + rec.get(c).length() + " c is " + c);
                System.arraycopy(rec.get(c).array(), rec.get(c).offset(), buf.array(), buf.offset() + pos, rec.get(c).length());
            } else {
                Arrays.fill(buf.array(), buf.offset() + pos, buf.offset() + pos + colLength, fill);
            }
            pos += colLength;
        }
        buf.setLength(pos);

        return buf;
    }

    /**
     * every column in scan key is fixed length. for fixed columns, 0 will be populated, for non-fixed columns, 1 will be populated 
     */
    public static ByteArray exportScanMask(GTRecord rec) {
        Preconditions.checkNotNull(rec);

        GTInfo info = rec.getInfo();
        int len = info.getMaxColumnLength(info.getPrimaryKey());
        ByteArray buf = ByteArray.allocate(len);
        byte fill;

        int pos = 0;
        for (int i = 0; i < info.getPrimaryKey().trueBitCount(); i++) {
            int c = info.getPrimaryKey().trueBitAt(i);
            int colLength = info.getCodeSystem().maxCodeLength(c);

            if (rec.get(c).array() != null) {
                fill = RowConstants.BYTE_ZERO;
            } else {
                fill = RowConstants.BYTE_ONE;
            }
            Arrays.fill(buf.array(), buf.offset() + pos, buf.offset() + pos + colLength, fill);
            pos += colLength;
        }
        buf.setLength(pos);

        return buf;
    }
}
