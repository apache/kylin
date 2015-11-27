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

package org.apache.kylin.cube.model;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * @author yangli9
 * 
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class RowKeyColDesc {

    public enum ColEncodingType {
        DICT, FIXED_LEN
    }

    public class ColEncoding {
        public ColEncodingType type;
        public Object param;

        public ColEncoding(ColEncodingType type, Object param) {
            this.type = type;
            this.param = param;
        }
    }

    @JsonProperty("column")
    private String column;
    @JsonProperty("encoding")
    private String encoding;

    // computed
    private ColEncoding colEncoding;
    private int bitIndex;
    private TblColRef colRef;

    public void init() {

        //dict or fix length?
        Preconditions.checkState(StringUtils.isNotEmpty(this.encoding));
        if (this.encoding.equalsIgnoreCase("dict")) {
            this.colEncoding = new ColEncoding(ColEncodingType.DICT, null);
        } else if (this.encoding.startsWith("fixed_length")) {
            int length = RowConstants.ROWKEY_COL_DEFAULT_LENGTH;
            if (this.encoding.indexOf(":") > 0) {
                length = Integer.parseInt(this.encoding.substring(this.encoding.indexOf(":") + 1));
            }
            this.colEncoding = new ColEncoding(ColEncodingType.FIXED_LEN, length);
        } else {
            throw new IllegalArgumentException("Not supported row key col encoding:" + this.encoding);
        }
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public boolean isUsingDictionary() {
        return this.colEncoding.type == ColEncodingType.DICT;

    }

    public int getLength() {
        if (this.colEncoding.type == ColEncodingType.FIXED_LEN) {
            return (Integer) this.colEncoding.param;
        } else {
            return 0;
        }
    }

    public int getBitIndex() {
        return bitIndex;
    }

    void setBitIndex(int index) {
        this.bitIndex = index;
    }

    public TblColRef getColRef() {
        return colRef;
    }

    void setColRef(TblColRef colRef) {
        this.colRef = colRef;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("column", column).add("encoding", encoding).toString();
    }

}
