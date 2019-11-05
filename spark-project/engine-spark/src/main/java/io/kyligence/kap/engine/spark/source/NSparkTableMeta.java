/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.engine.spark.source;

import java.util.List;

public class NSparkTableMeta {
    static class SparkTableColumnMeta {
        String name;
        String dataType;
        String comment;

        public SparkTableColumnMeta(String name, String dataType, String comment) {
            this.name = name;
            this.dataType = dataType;
            this.comment = comment;
        }

        @Override
        public String toString() {
            return "SparkTableColumnMeta{" + "name='" + name + '\'' + ", dataType='" + dataType + '\'' + ", comment='"
                    + comment + '\'' + '}';
        }
    }

    String tableName;
    String sdLocation;//sd is short for storage descriptor
    String sdInputFormat;
    String sdOutputFormat;
    String owner;
    String provider;
    String tableType;
    String createTime;
    String lastAccessTime;
    long fileSize;
    long fileNum;
    boolean isNative;
    List<SparkTableColumnMeta> allColumns;
    List<SparkTableColumnMeta> partitionColumns;

    public NSparkTableMeta(String tableName, String sdLocation, String sdInputFormat, String sdOutputFormat,
            String owner, String provider, String tableType, String createTime, String lastAccessTime, long fileSize,
            long fileNum, boolean isNative, List<SparkTableColumnMeta> allColumns,
            List<SparkTableColumnMeta> partitionColumns) {
        this.tableName = tableName;
        this.sdLocation = sdLocation;
        this.sdInputFormat = sdInputFormat;
        this.sdOutputFormat = sdOutputFormat;
        this.owner = owner;
        this.provider = provider;
        this.tableType = tableType;
        this.createTime = createTime;
        this.lastAccessTime = lastAccessTime;
        this.fileSize = fileSize;
        this.fileNum = fileNum;
        this.isNative = isNative;
        this.allColumns = allColumns;
        this.partitionColumns = partitionColumns;
    }

    @Override
    public String toString() {
        return "SparkTableMeta{" + "tableName='" + tableName + '\'' + ", sdLocation='" + sdLocation + '\''
                + ", sdInputFormat='" + sdInputFormat + '\'' + ", sdOutputFormat='" + sdOutputFormat + '\''
                + ", owner='" + owner + ", provider='" + provider + '\'' + ", tableType='" + tableType
                + ", createTime='" + createTime + '\'' + ", lastAccessTime=" + lastAccessTime + ", fileSize=" + fileSize
                + ", fileNum=" + fileNum + ", isNative=" + isNative + ", allColumns=" + allColumns
                + ", partitionColumns=" + partitionColumns + '}';
    }
}
