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

public class TimedRecordEvent extends RecordEvent {

    public TimedRecordEvent(String eventType) {
        super(eventType);

        //Add time details
        addTimeDetails();
    }

    private void addTimeDetails() {
        RecordEventTimeDetail dateDetail = new RecordEventTimeDetail(getTime());
        put(TimePropertyEnum.YEAR.toString(), dateDetail.year_begin_date);
        put(TimePropertyEnum.MONTH.toString(), dateDetail.month_begin_date);
        put(TimePropertyEnum.WEEK_BEGIN_DATE.toString(), dateDetail.week_begin_date);
        put(TimePropertyEnum.DAY_DATE.toString(), dateDetail.date);
        put(TimePropertyEnum.DAY_TIME.toString(), dateDetail.time);
        put(TimePropertyEnum.TIME_HOUR.toString(), dateDetail.hour);
        put(TimePropertyEnum.TIME_MINUTE.toString(), dateDetail.minute);
        put(TimePropertyEnum.TIME_SECOND.toString(), dateDetail.second);
    }

    @Override
    public void resetTime() {
        super.resetTime();
        addTimeDetails();
    }
}
