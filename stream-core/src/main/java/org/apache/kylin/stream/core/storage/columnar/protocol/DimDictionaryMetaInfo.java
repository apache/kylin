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

package org.apache.kylin.stream.core.storage.columnar.protocol;

public class DimDictionaryMetaInfo {
    // Name of the dimension column
    private String dimName;

    // The start of the dimension dictionary data within the file
    private int startOffset;

    // Dictionary type
    private String dictType;

    // Dictionary part length in bytes
    private int dictLength;

    public String getDimName() {
        return dimName;
    }

    public void setDimName(String dimName) {
        this.dimName = dimName;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(int startOffset) {
        this.startOffset = startOffset;
    }

    public String getDictType() {
        return dictType;
    }

    public void setDictType(String dictType) {
        this.dictType = dictType;
    }

    public int getDictLength() {
        return dictLength;
    }

    public void setDictLength(int dictLength) {
        this.dictLength = dictLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DimDictionaryMetaInfo that = (DimDictionaryMetaInfo) o;

        if (startOffset != that.startOffset)
            return false;
        if (dictLength != that.dictLength)
            return false;
        if (dimName != null ? !dimName.equals(that.dimName) : that.dimName != null)
            return false;
        return dictType != null ? dictType.equals(that.dictType) : that.dictType == null;

    }

    @Override
    public int hashCode() {
        int result = dimName != null ? dimName.hashCode() : 0;
        result = 31 * result + startOffset;
        result = 31 * result + (dictType != null ? dictType.hashCode() : 0);
        result = 31 * result + dictLength;
        return result;
    }
}
