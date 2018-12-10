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

/**
 * the cuboid metadata in the fragment
 * 
 */
public class CuboidMetaInfo {
    // Dimension info, data are divided per dimension. Each dimension contains
    // three types of data: dictionary, raw data and inverted indexes.
    private List<DimensionMetaInfo> dimensionsInfo;

    // Metric info, different metric data stores in different position of the file
    private List<MetricMetaInfo> metricsInfo;

    // Number of dimensions
    private int numberOfDim;

    // Number of metrics
    private int numberOfMetrics;

    // Number of rows in the fragment cuboid data
    private long numberOfRows;

    public List<DimensionMetaInfo> getDimensionsInfo() {
        return dimensionsInfo;
    }

    public void setDimensionsInfo(List<DimensionMetaInfo> dimensionsInfo) {
        this.dimensionsInfo = dimensionsInfo;
    }

    public List<MetricMetaInfo> getMetricsInfo() {
        return metricsInfo;
    }

    public void setMetricsInfo(List<MetricMetaInfo> metricsInfo) {
        this.metricsInfo = metricsInfo;
    }

    public int getNumberOfMetrics() {
        return numberOfMetrics;
    }

    public void setNumberOfMetrics(int numberOfMetrics) {
        this.numberOfMetrics = numberOfMetrics;
    }

    public int getNumberOfDim() {
        return numberOfDim;
    }

    public void setNumberOfDim(int numberOfDim) {
        this.numberOfDim = numberOfDim;
    }

    public long getNumberOfRows() {
        return numberOfRows;
    }

    public void setNumberOfRows(long numberOfRows) {
        this.numberOfRows = numberOfRows;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CuboidMetaInfo that = (CuboidMetaInfo) o;

        if (numberOfDim != that.numberOfDim)
            return false;
        if (numberOfMetrics != that.numberOfMetrics)
            return false;
        if (numberOfRows != that.numberOfRows)
            return false;
        if (dimensionsInfo != null ? !dimensionsInfo.equals(that.dimensionsInfo) : that.dimensionsInfo != null)
            return false;
        return metricsInfo != null ? metricsInfo.equals(that.metricsInfo) : that.metricsInfo == null;

    }

    @Override
    public int hashCode() {
        int result = dimensionsInfo != null ? dimensionsInfo.hashCode() : 0;
        result = 31 * result + (metricsInfo != null ? metricsInfo.hashCode() : 0);
        result = 31 * result + numberOfDim;
        result = 31 * result + numberOfMetrics;
        result = 31 * result + (int) (numberOfRows ^ (numberOfRows >>> 32));
        return result;
    }
}
