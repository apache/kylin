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

/**
 * The Footer section of the file provides the necessary information to interpret the rest of the file including the version info and the length of the segment meta.
 *  
 *
 */
public class Footer {
    //Version of the storage format for the segment
    private int version;

    //The start of the segmentMetaInfo within the file
    private long segmentInfoOffSet;

    //The length of the segmentMetaInfo in bytes
    private long segmentInfoLength;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getSegmentInfoOffSet() {
        return segmentInfoOffSet;
    }

    public void setSegmentInfoOffSet(long segmentInfoOffSet) {
        this.segmentInfoOffSet = segmentInfoOffSet;
    }

    public long getSegmentInfoLength() {
        return segmentInfoLength;
    }

    public void setSegmentInfoLength(long segmentInfoLength) {
        this.segmentInfoLength = segmentInfoLength;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (segmentInfoLength ^ (segmentInfoLength >>> 32));
        result = prime * result + (int) (segmentInfoOffSet ^ (segmentInfoOffSet >>> 32));
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Footer other = (Footer) obj;
        if (segmentInfoLength != other.segmentInfoLength)
            return false;
        if (segmentInfoOffSet != other.segmentInfoOffSet)
            return false;
        if (version != other.version)
            return false;
        return true;
    }
}
