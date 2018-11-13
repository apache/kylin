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

package org.apache.kylin.storage.parquet.spark;

import java.util.List;

public class ParquetPayload {
    private byte[] gtScanRequest;
    private String gtScanRequestId;
    private String kylinProperties;
    private String realizationId;
    private String segmentId;
    private String dataFolderName;
    private int maxRecordLength;
    private List<Integer> parquetColumns;
    private boolean isUseII;
    private String realizationType;
    private String queryId;
    private boolean spillEnabled;
    private long maxScanBytes;
    private long startTime;
    private int storageType;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private ParquetPayload(byte[] gtScanRequest, String gtScanRequestId, String kylinProperties, String realizationId,
                           String segmentId, String dataFolderName, int maxRecordLength, List<Integer> parquetColumns,
                           boolean isUseII, String realizationType, String queryId, boolean spillEnabled, long maxScanBytes,
                           long startTime, int storageType) {
        this.gtScanRequest = gtScanRequest;
        this.gtScanRequestId = gtScanRequestId;
        this.kylinProperties = kylinProperties;
        this.realizationId = realizationId;
        this.segmentId = segmentId;
        this.dataFolderName = dataFolderName;
        this.maxRecordLength = maxRecordLength;
        this.parquetColumns = parquetColumns;
        this.isUseII = isUseII;
        this.realizationType = realizationType;
        this.queryId = queryId;
        this.spillEnabled = spillEnabled;
        this.maxScanBytes = maxScanBytes;
        this.startTime = startTime;
        this.storageType = storageType;
    }

    public byte[] getGtScanRequest() {
        return gtScanRequest;
    }

    public String getGtScanRequestId() {
        return gtScanRequestId;
    }

    public String getKylinProperties() {
        return kylinProperties;
    }

    public String getRealizationId() {
        return realizationId;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public String getDataFolderName() {
        return dataFolderName;
    }

    public int getMaxRecordLength() {
        return maxRecordLength;
    }

    public List<Integer> getParquetColumns() {
        return parquetColumns;
    }

    public boolean isUseII() {
        return isUseII;
    }

    public String getRealizationType() {
        return realizationType;
    }

    public String getQueryId() {
        return queryId;
    }

    public boolean isSpillEnabled() {
        return spillEnabled;
    }

    public long getMaxScanBytes() {
        return maxScanBytes;
    }

    public long getStartTime() {
        return startTime;
    }

    public int getStorageType() {
        return storageType;
    }

    static public class ParquetPayloadBuilder {
        private byte[] gtScanRequest;
        private String gtScanRequestId;
        private String kylinProperties;
        private String realizationId;
        private String segmentId;
        private String dataFolderName;
        private int maxRecordLength;
        private List<Integer> parquetColumns;
        private boolean isUseII;
        private String realizationType;
        private String queryId;
        private boolean spillEnabled;
        private long maxScanBytes;
        private long startTime;
        private int storageType;

        public ParquetPayloadBuilder() {
        }

        public ParquetPayloadBuilder setGtScanRequest(byte[] gtScanRequest) {
            this.gtScanRequest = gtScanRequest;
            return this;
        }

        public ParquetPayloadBuilder setGtScanRequestId(String gtScanRequestId) {
            this.gtScanRequestId = gtScanRequestId;
            return this;
        }

        public ParquetPayloadBuilder setKylinProperties(String kylinProperties) {
            this.kylinProperties = kylinProperties;
            return this;
        }

        public ParquetPayloadBuilder setRealizationId(String realizationId) {
            this.realizationId = realizationId;
            return this;
        }

        public ParquetPayloadBuilder setSegmentId(String segmentId) {
            this.segmentId = segmentId;
            return this;
        }

        public ParquetPayloadBuilder setDataFolderName(String dataFolderName) {
            this.dataFolderName = dataFolderName;
            return this;
        }

        public ParquetPayloadBuilder setMaxRecordLength(int maxRecordLength) {
            this.maxRecordLength = maxRecordLength;
            return this;
        }

        public ParquetPayloadBuilder setParquetColumns(List<Integer> parquetColumns) {
            this.parquetColumns = parquetColumns;
            return this;
        }

        public ParquetPayloadBuilder setIsUseII(boolean isUseII) {
            this.isUseII = isUseII;
            return this;
        }

        public ParquetPayloadBuilder setRealizationType(String realizationType) {
            this.realizationType = realizationType;
            return this;
        }

        public ParquetPayloadBuilder setQueryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public ParquetPayloadBuilder setSpillEnabled(boolean spillEnabled) {
            this.spillEnabled = spillEnabled;
            return this;
        }

        public ParquetPayloadBuilder setMaxScanBytes(long maxScanBytes) {
            this.maxScanBytes = maxScanBytes;
            return this;
        }

        public ParquetPayloadBuilder setStartTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public ParquetPayloadBuilder setStorageType(int storageType) {
            this.storageType = storageType;
            return this;
        }

        public ParquetPayload createParquetPayload() {
            return new ParquetPayload(gtScanRequest, gtScanRequestId, kylinProperties, realizationId, segmentId,
                    dataFolderName, maxRecordLength, parquetColumns, isUseII, realizationType, queryId, spillEnabled,
                    maxScanBytes, startTime, storageType);
        }
    }
}
