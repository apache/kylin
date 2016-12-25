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

package org.apache.kylin.cube.kv;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Read/Write column values from/into bytes
 *
 * @author yangli9
 */
public class RowKeyColumnIO implements java.io.Serializable {

    //private static final Logger logger = LoggerFactory.getLogger(RowKeyColumnIO.class);

    private final IDimensionEncodingMap dimEncMap;

    public RowKeyColumnIO(IDimensionEncodingMap dimEncMap) {
        this.dimEncMap = dimEncMap;
    }

    public int getColumnLength(TblColRef col) {
        return dimEncMap.get(col).getLengthOfEncoding();
    }

    public Dictionary<String> getDictionary(TblColRef col) {
        return dimEncMap.getDictionary(col);
    }

    public void writeColumn(TblColRef col, String value, int roundingFlag, byte defaultValue, byte[] output, int outputOffset) {
        DimensionEncoding dimEnc = dimEncMap.get(col);
        if (dimEnc instanceof DictionaryDimEnc)
            dimEnc = ((DictionaryDimEnc) dimEnc).copy(roundingFlag, defaultValue);

        dimEnc.encode(value, output, outputOffset);
    }

    public String readColumnString(TblColRef col, byte[] bytes, int offset, int length) {
        DimensionEncoding dimEnc = dimEncMap.get(col);
        return dimEnc.decode(bytes, offset, length);
    }

}
