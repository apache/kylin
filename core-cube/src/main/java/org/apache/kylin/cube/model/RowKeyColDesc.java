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
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncodingFactory;
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

    @JsonProperty("column")
    private String column;
    @JsonProperty("encoding")
    private String encoding;
    @JsonProperty("isShardBy")
    private boolean isShardBy;//usually it is ultra high cardinality column, shard by such column can reduce the agg cache for each shard
    @JsonProperty("index")
    private String index;

    // computed
    private String encodingName;
    private String[] encodingArgs;
    private int bitIndex;
    private TblColRef colRef;

    public void init() {
        Preconditions.checkState(StringUtils.isNotEmpty(this.encoding));

        String[] parts = this.encoding.split("\\s*[(),:]\\s*");
        if (parts == null || parts.length == 0 || parts[0].isEmpty())
            throw new IllegalArgumentException("Not supported row key col encoding: '" + this.encoding + "'");

        this.encodingName = parts[0];
        this.encodingArgs = parts[parts.length - 1].isEmpty() //
                ? StringUtil.subArray(parts, 1, parts.length - 1) : StringUtil.subArray(parts, 1, parts.length);

        if (!DimensionEncodingFactory.isVaildEncoding(this.encodingName))
            throw new IllegalArgumentException("Not supported row key col encoding: '" + this.encoding + "'");
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

    public boolean isShardBy() {
        return isShardBy;
    }

    public void setShardBy(boolean shardBy) {
        isShardBy = shardBy;
    }

    public String getEncodingName() {
        return encodingName;
    }

    public String[] getEncodingArgs() {
        return encodingArgs;
    }

    public boolean isUsingDictionary() {
        return DictionaryDimEnc.ENCODING_NAME.equals(encodingName);
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

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("column", column).add("encoding", encoding).toString();
    }

}