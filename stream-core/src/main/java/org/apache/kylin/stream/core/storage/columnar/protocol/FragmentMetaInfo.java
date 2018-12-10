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

import java.util.List;
import java.util.Map;

/**
 * The fragment metadata, it will be stored in the file in JSON format. It
 * contains the layout of the body of the file. The body of the file is divided
 * into different parts to organize the data in columnar format.
 * 
 */
public class FragmentMetaInfo {

    private List<DimDictionaryMetaInfo> dimDictionaryMetaInfos;

    private CuboidMetaInfo basicCuboidMetaInfo;

    // Cuboids mataData info
    private Map<String, CuboidMetaInfo> cuboidMetaInfoMap;

    // Number of rows in the fragment
    private long numberOfRows;

    // Original rows number from source
    private long originNumOfRows;

    // FragmentId
    private String fragmentId;

    private long minEventTime;

    private long maxEventTime;

    // Version of the fragment
    private int version;

    public List<DimDictionaryMetaInfo> getDimDictionaryMetaInfos() {
        return dimDictionaryMetaInfos;
    }

    public void setDimDictionaryMetaInfos(List<DimDictionaryMetaInfo> dimDictionaryMetaInfos) {
        this.dimDictionaryMetaInfos = dimDictionaryMetaInfos;
    }

    public Map<String, CuboidMetaInfo> getCuboidMetaInfoMap() {
        return cuboidMetaInfoMap;
    }

    public void setCuboidMetaInfoMap(Map<String, CuboidMetaInfo> cuboidMetaInfoMap) {
        this.cuboidMetaInfoMap = cuboidMetaInfoMap;
    }

    public CuboidMetaInfo getBasicCuboidMetaInfo() {
        return basicCuboidMetaInfo;
    }

    public void setBasicCuboidMetaInfo(CuboidMetaInfo basicCuboidMetaInfo) {
        this.basicCuboidMetaInfo = basicCuboidMetaInfo;
    }

    public CuboidMetaInfo getCuboidMetaInfo(long cuboidID) {
        return cuboidMetaInfoMap.get(String.valueOf(cuboidID));
    }

    public long getNumberOfRows() {
        return numberOfRows;
    }

    public void setNumberOfRows(long numberOfRows) {
        this.numberOfRows = numberOfRows;
    }

    public long getOriginNumOfRows() {
        return originNumOfRows;
    }

    public void setOriginNumOfRows(long originNumOfRows) {
        this.originNumOfRows = originNumOfRows;
    }

    public String getFragmentId() {
        return fragmentId;
    }

    public void setFragmentId(String fragmentId) {
        this.fragmentId = fragmentId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getMinEventTime() {
        return minEventTime;
    }

    public void setMinEventTime(long minEventTime) {
        this.minEventTime = minEventTime;
    }

    public long getMaxEventTime() {
        return maxEventTime;
    }

    public void setMaxEventTime(long maxEventTime) {
        this.maxEventTime = maxEventTime;
    }
    
    public boolean hasValidEventTimeRange() {
        return minEventTime != 0 && maxEventTime != 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        FragmentMetaInfo that = (FragmentMetaInfo) o;

        if (numberOfRows != that.numberOfRows)
            return false;
        if (version != that.version)
            return false;
        if (dimDictionaryMetaInfos != null ? !dimDictionaryMetaInfos.equals(that.dimDictionaryMetaInfos)
                : that.dimDictionaryMetaInfos != null)
            return false;
        if (basicCuboidMetaInfo != null ? !basicCuboidMetaInfo.equals(that.basicCuboidMetaInfo)
                : that.basicCuboidMetaInfo != null)
            return false;
        if (cuboidMetaInfoMap != null ? !cuboidMetaInfoMap.equals(that.cuboidMetaInfoMap)
                : that.cuboidMetaInfoMap != null)
            return false;
        return fragmentId != null ? fragmentId.equals(that.fragmentId) : that.fragmentId == null;

    }

    @Override
    public int hashCode() {
        int result = dimDictionaryMetaInfos != null ? dimDictionaryMetaInfos.hashCode() : 0;
        result = 31 * result + (basicCuboidMetaInfo != null ? basicCuboidMetaInfo.hashCode() : 0);
        result = 31 * result + (cuboidMetaInfoMap != null ? cuboidMetaInfoMap.hashCode() : 0);
        result = 31 * result + (int) (numberOfRows ^ (numberOfRows >>> 32));
        result = 31 * result + (fragmentId != null ? fragmentId.hashCode() : 0);
        result = 31 * result + version;
        return result;
    }
}
