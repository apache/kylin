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

package org.apache.kylin.metrics.lib.impl;

import java.io.Serializable;

import org.apache.kylin.metrics.lib.Record;

public class RecordEventWrapper implements Serializable {

    protected final RecordEvent metricsEvent;

    public RecordEventWrapper(RecordEvent metricsEvent) {
        this.metricsEvent = metricsEvent;

        //Add time details
        addTimeDetails();
    }

    private void addTimeDetails() {
        RecordEventTimeDetail dateDetail = new RecordEventTimeDetail(metricsEvent.getTime());
        metricsEvent.put(TimePropertyEnum.YEAR.toString(), dateDetail.year_begin_date);
        metricsEvent.put(TimePropertyEnum.MONTH.toString(), dateDetail.month_begin_date);
        metricsEvent.put(TimePropertyEnum.WEEK_BEGIN_DATE.toString(), dateDetail.week_begin_date);
        metricsEvent.put(TimePropertyEnum.DAY_DATE.toString(), dateDetail.date);
        metricsEvent.put(TimePropertyEnum.DAY_TIME.toString(), dateDetail.time);
        metricsEvent.put(TimePropertyEnum.TIME_HOUR.toString(), dateDetail.hour);
        metricsEvent.put(TimePropertyEnum.TIME_MINUTE.toString(), dateDetail.minute);
        metricsEvent.put(TimePropertyEnum.TIME_SECOND.toString(), dateDetail.second);
    }

    public void resetTime() {
        metricsEvent.resetTime();
        addTimeDetails();
    }

    public Record getMetricsRecord() {
        return metricsEvent;
    }

    @Override
    public String toString() {
        return metricsEvent.toString();
    }
}
