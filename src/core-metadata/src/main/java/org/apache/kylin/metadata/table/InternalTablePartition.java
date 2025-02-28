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

package org.apache.kylin.metadata.table;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.util.DateFormat;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class InternalTablePartition implements Serializable {

    @JsonProperty("partition_columns")
    private String[] partitionColumns;

    // Only record level one partition values for date partition
    @JsonProperty("partition_values")
    private List<String> partitionValues;

    @JsonProperty("date_partition_format")
    private String datePartitionFormat;

    @JsonProperty("partition_details")
    private List<InternalTablePartitionDetail> partitionDetails;

    public InternalTablePartition(String[] partitionColumns, String datePartitionFormat) {
        this.partitionColumns = partitionColumns;
        this.datePartitionFormat = datePartitionFormat;
    }

    public static class DefaultPartitionConditionBuilder {

        public static String buildDateRangeCondition(String datePartitionColumnName, String dateFormat,
                String startDate, String endDate) {
            return String.format(Locale.ROOT, "%s >= to_date('%s', '%s') AND %s < to_date('%s', '%s')",
                    datePartitionColumnName, DateFormat.formatToDateStr(Long.parseLong(startDate), dateFormat),
                    dateFormat, datePartitionColumnName,
                    DateFormat.formatToDateStr(Long.parseLong(endDate), dateFormat), dateFormat);
        }

        public String buildTimeRangeCondition(String timePartitionColumnName, String startTime, String endTime) {
            return String.format(Locale.ROOT, "%s >= '%s' AND %s < '%s'", timePartitionColumnName, startTime,
                    timePartitionColumnName, endTime);
        }

        public String buildTimeRangeCondition(String timePartitionColumnName, Long startTime, Long endTime) {
            return String.format(Locale.ROOT, "%s >= %d AND %s < %d", timePartitionColumnName, startTime,
                    timePartitionColumnName, endTime);
        }
    }

}
