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

public class MetricMetaInfo {
    // Name of the measure column
    private String name;

    // Compression type
    private String compression;

    // Integer of the column index
    private int col;

    // include null values or not
    private boolean hasNull;

    // The start of the measure data within the file
    private int startOffset;

    private int metricLength;

    //the max number of bytes to the longest possible serialization of this metric data type
    private int maxSerializeLength;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCol() {
        return col;
    }

    public void setCol(int col) {
        this.col = col;
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

    public int getMetricLength() {
        return metricLength;
    }

    public void setMetricLength(int metricLength) {
        this.metricLength = metricLength;
    }

    public int getMaxSerializeLength() {
        return this.maxSerializeLength;
    }

    public void setMaxSerializeLength(int length) {
        this.maxSerializeLength = length;
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

        MetricMetaInfo that = (MetricMetaInfo) o;

        if (col != that.col)
            return false;
        if (hasNull != that.hasNull)
            return false;
        if (startOffset != that.startOffset)
            return false;
        if (metricLength != that.metricLength)
            return false;
        if (maxSerializeLength != that.maxSerializeLength)
            return false;
        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;
        return compression != null ? compression.equals(that.compression) : that.compression == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (compression != null ? compression.hashCode() : 0);
        result = 31 * result + col;
        result = 31 * result + (hasNull ? 1 : 0);
        result = 31 * result + startOffset;
        result = 31 * result + metricLength;
        result = 31 * result + maxSerializeLength;
        return result;
    }
}
