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

package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

/**
 * 
 * @author xjiang
 * 
 */
public class ExtractTupleFilter extends TupleFilter {

    private int date;
    private List<Object> values;

    public ExtractTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(3), op);
        assert (op == FilterOperatorEnum.EXTRACT);
        this.values = new ArrayList<Object>(1);
        this.values.add(null);
        this.date = 0;
    }

    @Override
    public String toString() {
        return "ExtractTupleFilter=[children=" + this.children + "]";
    }

    @Override
    public boolean isEvaluable() {
        return false;
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        // extract tuple value
        String extractType = null;
        String tupleValue = null;
        for (TupleFilter filter : this.children) {
            filter.evaluate(tuple, cs);
            if (filter instanceof ConstantTupleFilter) {
                tupleValue = filter.getValues().iterator().next().toString();
            } else if (filter instanceof CompareTupleFilter) {
                extractType = filter.getValues().iterator().next().toString();
            }
        }

        // extract date
        this.date = extractDate(extractType, Integer.parseInt(tupleValue));
        return true;
    }

    private int extractDate(String type, int inDate) {
        // this shifts the epoch back to astronomical year -4800 instead of the
        // start of the Christian era in year AD 1 of the proleptic Gregorian
        // calendar.
        int j = inDate + 32044;
        int g = j / 146097;
        int dg = j % 146097;
        int c = (dg / 36524 + 1) * 3 / 4;
        int dc = dg - c * 36524;
        int b = dc / 1461;
        int db = dc % 1461;
        int a = (db / 365 + 1) * 3 / 4;
        int da = db - a * 365;

        // integer number of full years elapsed since March 1, 4801 BC
        int y = g * 400 + c * 100 + b * 4 + a;
        // integer number of full months elapsed since the last March 1
        int m = (da * 5 + 308) / 153 - 2;
        // number of days elapsed since day 1 of the month
        int d = da - (m + 4) * 153 / 5 + 122;
        int year = y - 4800 + (m + 2) / 12;
        int month = (m + 2) % 12 + 1;
        int day = d + 1;
        if ("YEAR".equalsIgnoreCase(type)) {
            return year;
        }
        if ("MONTH".equalsIgnoreCase(type)) {
            return month;
        }
        if ("DAY".equalsIgnoreCase(type)) {
            return day;
        }
        return -1;
    }

    @Override
    public Collection<?> getValues() {
        this.values.set(0, String.valueOf(this.date));
        return this.values;
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        //do nothing
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
    }

}
