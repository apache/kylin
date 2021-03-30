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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.kylin.stream.core.storage.columnar.compress.Compression;

public class DimensionMetaInfo {
    // Name of the dimension column
    private String name;

    // Compression type
    private String compression;

    // include null values or not
    private boolean hasNull;

    // The start of the dimension data within the file
    private int startOffset;

    // Data part length in bytes
    private int dataLength;

    // Index part length in bytes
    private int indexLength;

    // Magic number for future use.
    private int magic;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isHasNull() {
        return hasNull;
    }

    public void setHasNull(boolean hasNull) {
        this.hasNull = hasNull;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(int startOffset) {
        this.startOffset = startOffset;
    }

    public int getDataLength() {
        return dataLength;
    }

    public void setDataLength(int dataLength) {
        this.dataLength = dataLength;
    }

    public int getIndexLength() {
        return indexLength;
    }

    public void setIndexLength(int indexLength) {
        this.indexLength = indexLength;
    }

    public int getMagic() {
        return magic;
    }

    public void setMagic(int magic) {
        this.magic = magic;
    }

    public String getCompression() {
        return compression;
    }

    @JsonIgnore
    public Compression getCompressionType() {
        return Compression.valueOf(compression);
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DimensionMetaInfo that = (DimensionMetaInfo) o;

        if (hasNull != that.hasNull)
            return false;
        if (startOffset != that.startOffset)
            return false;
        if (dataLength != that.dataLength)
            return false;
        if (indexLength != that.indexLength)
            return false;
        if (magic != that.magic)
            return false;
        return name != null ? name.equals(that.name) : that.name == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (hasNull ? 1 : 0);
        result = 31 * result + startOffset;
        result = 31 * result + dataLength;
        result = 31 * result + indexLength;
        result = 31 * result + magic;
        return result;
    }
}
