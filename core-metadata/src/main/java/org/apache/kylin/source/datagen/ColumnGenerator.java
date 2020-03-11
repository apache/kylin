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

package org.apache.kylin.source.datagen;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class ColumnGenerator {

    final private ColumnGenConfig conf;
    final private ColumnDesc targetCol;
    final private int targetRows;

    public ColumnGenerator(ColumnDesc col, int nRows, ModelDataGenerator modelGen) throws IOException {
        this.conf = new ColumnGenConfig(col, modelGen);
        this.targetCol = col;
        this.targetRows = nRows;
    }

    public Iterator<String> generate(long seed) {
        Base result;
        if (conf.isFK) {
            result = new DiscreteGen(conf.values, seed);
        } else if (conf.isID) {
            result = new IDGen(conf.idStart);
        } else if (conf.isRandom) {
            result = new RandomGen(targetCol, conf.randFormat, conf.randStart, conf.randEnd, conf.cardinality);
        } else {
            result = new DiscreteGen(conf.values);
        }

        if (conf.cardinality > 0) {
            result = new CardinalityFilter(result, conf.cardinality);
        }

        if (conf.genNull) {
            result = new AddNullFilter(result, conf.genNullPct, conf.genNullStr);
        }

        if (conf.order || conf.unique) {
            result = new OrderFilter(result, conf.unique, targetRows);
        }

        return result;
    }

    abstract public static class Base implements Iterator<String> {
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class RandomGen extends Base {

        private DataType type;
        private String format;
        private int randStart;
        private int randEnd;
        private Random rand;

        public RandomGen(ColumnDesc col, String format, int randStart, int randEnd, int cardinality) {
            this.type = col.getType();

            if (type.isStringFamily()) {
                // string
                if (StringUtils.isBlank(format)) {
                    String name = col.getName();
                    format = name.substring(0, Math.min(4, name.length())) + ColumnGenConfig.$RANDOM;
                }
                Preconditions.checkArgument(format.contains(ColumnGenConfig.$RANDOM));
                initNumberRange(randStart, randEnd, cardinality);
            } else if (type.isTimeFamily()) {
                // time
                format = StringUtil.noBlank(format, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
                initDateTimeRange(randStart, randEnd, 0);
            } else if (type.isDateTimeFamily()) {
                // date
                format = StringUtil.noBlank(format, DateFormat.DEFAULT_DATE_PATTERN);
                initDateTimeRange(randStart, randEnd, cardinality);
            } else if (type.isIntegerFamily()) {
                // integer
                initNumberRange(randStart, randEnd, cardinality);
                format = StringUtil.noBlank(format, "#");
            } else if (type.isNumberFamily()) {
                // double
                initNumberRange(randStart, randEnd, 0);
                format = StringUtil.noBlank(format, ".##");
            } else {
                throw new IllegalArgumentException();
            }

            this.format = format;
            this.rand = new Random();
        }

        private void initDateTimeRange(int randStart, int randEnd, int days) {
            if (randStart == 0 && randEnd == 0) {
                randStart = 2010;
                randEnd = 2015;
            }
            randEnd = Math.max(randEnd, randStart + (days / 365) + 1);

            Preconditions.checkArgument(randStart < randEnd);
            Preconditions.checkArgument((randEnd - randStart) * 365 >= days);

            this.randStart = randStart;
            this.randEnd = randEnd;
        }

        private void initNumberRange(int randStart, int randEnd, int cardinality) {
            if (randStart == 0 && randEnd == 0) {
                randStart = 0;
                randEnd = 1000;
            }
            randEnd = Math.max(randEnd, randStart + cardinality);

            Preconditions.checkArgument(randStart < randEnd);
            Preconditions.checkArgument(randEnd - randStart >= cardinality);

            this.randStart = randStart;
            this.randEnd = randEnd;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            if (type.isStringFamily()) {
                // string
                return format.replace(ColumnGenConfig.$RANDOM, "" + randomInt());
            } else if (type.isTimeFamily()) {
                // time
                return DateFormat.formatToTimeStr(randomMillis(), format);
            } else if (type.isDateTimeFamily()) {
                // date
                return DateFormat.formatToDateStr(randomMillis(), format);
            } else if (type.isIntegerFamily()) {
                // integer
                return formatNumber(randomInt());
            } else if (type.isNumberFamily()) {
                // double
                return formatNumber(randomDouble());
            } else {
                throw new NoSuchElementException();
            }
        }

        private String formatNumber(double i) {
            return new DecimalFormat(format, DecimalFormatSymbols.getInstance(Locale.ROOT)).format(i);
        }

        private int randomInt() {
            return randStart + rand.nextInt(randEnd - randStart);
        }

        private double randomDouble() {
            return randomInt() + rand.nextDouble();
        }

        private long randomMillis() {
            int secondsInYear = 3600 * 24 * 365;
            long year = randStart + rand.nextInt(randEnd - randStart) - 1970L;
            long second = year * secondsInYear + rand.nextInt(secondsInYear);
            return second * 1000;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private class IDGen extends Base {

        int next;

        public IDGen(int start) {
            next = start;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return "" + (next++);
        }
    }

    private class DiscreteGen extends Base {

        private List<String> values;
        private Random rand;

        public DiscreteGen(List<String> values) {
            this.values = values;
            this.rand = new Random();
        }

        public DiscreteGen(List<String> values, long seed) {
            this.values = values;
            this.rand = new Random(seed);
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            if (values.isEmpty())
                throw new NoSuchElementException();

            return values.get(rand.nextInt(values.size()));
        }
    }

    private class CardinalityFilter extends Base {

        private Iterator<String> input;
        private int card;
        private TreeSet<String> cache;

        public CardinalityFilter(Iterator<String> input, int card) {
            assert card > 0;
            this.input = input;
            this.card = card;
            this.cache = new TreeSet<String>();
        }

        @Override
        public boolean hasNext() {
            return input.hasNext();
        }

        @Override
        public String next() {
            String r = input.next();

            if (cache.size() < card) {
                cache.add(r);
                return r;
            }

            r = cache.floor(r);
            return r == null ? cache.first() : r;
        }
    }

    private class AddNullFilter extends Base {

        private Iterator<String> input;
        private double nullPct;
        private String nullStr;
        private Random rand;

        public AddNullFilter(Iterator<String> input, double nullPct, String nullStr) {
            this.input = input;
            this.nullPct = nullPct;
            this.nullStr = nullStr;
            this.rand = new Random();
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            String r = nullStr;
            if (input.hasNext()) {
                r = input.next();
            }

            if (rand.nextDouble() < nullPct) {
                r = nullStr;
            }
            return r;
        }
    }

    final private Comparator<String> comp = new Comparator<String>() {
        @Override
        public int compare(String s1, String s2) {
            if (s1 == null) {
                return s2 == null ? 0 : -1;
            } else if (s2 == null) {
                return 1;
            } else {
                if (targetCol.getType().isNumberFamily())
                    return Double.compare(Double.parseDouble(s1), Double.parseDouble(s2));
                else
                    return s1.compareTo(s2);
            }
        }
    };

    private class OrderFilter extends Base {

        private Iterator<String> iter;

        public OrderFilter(Iterator<String> input, boolean unique, int targetRows) {
            Collection<String> cache = unique ? new TreeSet<String>(comp) : new ArrayList<String>(targetRows);
            int cap = targetRows * 100;
            for (int i = 0; cache.size() < targetRows; i++) {
                cache.add(input.next());
                if (i >= cap)
                    throw new IllegalStateException();
            }

            if (cache instanceof List) {
                Collections.sort((List<String>) cache, comp);
            }

            iter = cache.iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public String next() {
            return iter.next();
        }
    }
}
