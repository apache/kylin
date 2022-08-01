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

package org.apache.kylin.common.metrics.reporter;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.metrics.service.InfluxDBInstance;
import org.apache.kylin.common.util.AddressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

public class InfluxdbReporter extends ScheduledReporter {

    private static final Logger logger = LoggerFactory.getLogger(InfluxdbReporter.class);

    private final ConcurrentHashMap<String, PointBuilder.Point> metrics = new ConcurrentHashMap<>();

    private final InfluxDBInstance influxInstance;

    private final Clock clock;

    private final Transformer transformer;

    private final String defaultMeasurement;

    private final String host;

    private static final String COUNT = "count";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String MEAN = "mean";
    private static final String STANDARD_DEVIATION = "std-dev";
    private static final String FIFTY_PERCENTILE = "50p";
    private static final String SEVENTY_FIVE_PERCENTILE = "75p";
    private static final String NINETY_FIVE_PERCENTILE = "95p";
    private static final String NINETY_NINE_PERCENTILE = "99p";
    private static final String NINETY_NINE_POINT_NINE_PERCENTILE = "999p";
    private static final String RUN_COUNT = "run-count";
    private static final String ONE_MINUTE = "1-minute";
    private static final String FIVE_MINUTE = "5-minute";
    private static final String FIFTEEN_MINUTE = "15-minute";
    private static final String MEAN_MINUTE = "mean-minute";

    public InfluxdbReporter(InfluxDBInstance influxInstance, String defaultMeasurement, MetricRegistry registry,
            String name) {
        super(registry, name, new ServerModeMetricFilter(), TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        this.influxInstance = influxInstance;
        this.clock = Clock.defaultClock();
        this.transformer = new Transformer();
        this.defaultMeasurement = defaultMeasurement;
        this.host = AddressUtil.getZkLocalInstance();
    }

    public InfluxDB getInfluxDb() {
        return this.influxInstance.getInfluxDB();
    }

    public ImmutableList<PointBuilder.Point> getMetrics() {
        return ImmutableList.copyOf(metrics.values());
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

        try {
            long startAt = System.currentTimeMillis();
            if (!getInfluxDb().ping().isGood()) {
                throw new IllegalStateException("the pinged influxdb is not good.");
            }

            final long timestamp = clock.getTime();

            final ImmutableList<PointBuilder.Point> points = ImmutableList.<PointBuilder.Point> builder()
                    .addAll(transformer.fromGauges(gauges, defaultMeasurement, timestamp, TimeUnit.MILLISECONDS))
                    .addAll(transformer.fromCounters(counters, defaultMeasurement, timestamp, TimeUnit.MILLISECONDS))
                    .addAll(transformer.fromHistograms(histograms, defaultMeasurement, timestamp,
                            TimeUnit.MILLISECONDS))
                    .addAll(transformer.fromMeters(meters, defaultMeasurement, timestamp, TimeUnit.MILLISECONDS))
                    .addAll(transformer.fromTimers(timers, defaultMeasurement, timestamp, TimeUnit.MILLISECONDS))
                    .build();

            points.stream().filter(Objects::nonNull).map(PointBuilder.Point::convert)
                    .forEach(p -> getInfluxDb().write(p));
            getInfluxDb().flush();

            MetricsGroup.counterInc(MetricsName.SUMMARY_COUNTER, MetricsCategory.HOST, host);
            MetricsGroup.counterInc(MetricsName.SUMMARY_DURATION, MetricsCategory.HOST, host,
                    System.currentTimeMillis() - startAt);

            points.stream().filter(Objects::nonNull).forEach(point -> metrics.putIfAbsent(point.getUniqueKey(), point));
            logger.debug("ke.metrics report data: {} points", points.size());
        } catch (Exception e) {
            logger.info("[UNEXPECTED_THINGS_HAPPENED] ke.metrics report data failed {}", e.getMessage());
        }
    }

    private class Transformer {

        public List<PointBuilder.Point> fromGauges(final Map<String, Gauge> gauges, final String measurement,
                final long timestamp, final TimeUnit timeUnit) {
            return fromGaugesOrCounters(gauges, Gauge::getValue, measurement, timestamp, timeUnit);
        }

        public List<PointBuilder.Point> fromCounters(final Map<String, Counter> counters, final String measurement,
                final long timestamp, final TimeUnit timeUnit) {
            return fromGaugesOrCounters(counters, Counter::getCount, measurement, timestamp, timeUnit);
        }

        public List<PointBuilder.Point> fromHistograms(final Map<String, Histogram> histograms,
                final String measurement, final long timestamp, final TimeUnit timeUnit) {

            return histograms.entrySet().stream().map(e -> {
                try {
                    Pair<String, Map<String, String>> nameTags = parseNameTags(e.getKey());
                    final Histogram histogram = e.getValue();
                    final Snapshot snapshot = histogram.getSnapshot();

                    return new PointBuilder(measurement, timestamp, timeUnit).putTags(nameTags.getSecond())
                            .putField(filedName(nameTags.getFirst(), COUNT), snapshot.size())
                            .putField(filedName(nameTags.getFirst(), MIN), snapshot.getMin())
                            .putField(filedName(nameTags.getFirst(), MAX), snapshot.getMax())
                            .putField(filedName(nameTags.getFirst(), MEAN), snapshot.getMean())
                            .putField(filedName(nameTags.getFirst(), STANDARD_DEVIATION), snapshot.getStdDev())
                            .putField(filedName(nameTags.getFirst(), FIFTY_PERCENTILE), snapshot.getMedian())
                            .putField(filedName(nameTags.getFirst(), SEVENTY_FIVE_PERCENTILE),
                                    snapshot.get75thPercentile())
                            .putField(filedName(nameTags.getFirst(), NINETY_FIVE_PERCENTILE),
                                    snapshot.get95thPercentile())
                            .putField(filedName(nameTags.getFirst(), NINETY_NINE_PERCENTILE),
                                    snapshot.get99thPercentile())
                            .putField(filedName(nameTags.getFirst(), NINETY_NINE_POINT_NINE_PERCENTILE),
                                    snapshot.get999thPercentile())
                            .putField(filedName(nameTags.getFirst(), RUN_COUNT), histogram.getCount()).build();
                } catch (Exception ex) {
                    logger.error("[UNEXPECTED_THINGS_HAPPENED] ke.metrics histogram {}", e.getKey(), ex);
                    return null;
                }
            }).filter(Objects::nonNull).collect(toList());
        }

        public List<PointBuilder.Point> fromMeters(final Map<String, Meter> meters, final String measurement,
                final long timestamp, final TimeUnit timeUnit) {
            return meters.entrySet().stream().map(e -> {
                try {
                    Pair<String, Map<String, String>> nameTags = parseNameTags(e.getKey());
                    final Meter meter = e.getValue();

                    return new PointBuilder(measurement, timestamp, timeUnit).putTags(nameTags.getSecond())
                            .putField(filedName(nameTags.getFirst(), COUNT), meter.getCount())
                            .putField(filedName(nameTags.getFirst(), ONE_MINUTE), convertRate(meter.getOneMinuteRate()))
                            .putField(filedName(nameTags.getFirst(), FIVE_MINUTE),
                                    convertRate(meter.getFiveMinuteRate()))
                            .putField(filedName(nameTags.getFirst(), FIFTEEN_MINUTE),
                                    convertRate(meter.getFifteenMinuteRate()))
                            .putField(filedName(nameTags.getFirst(), MEAN_MINUTE), convertRate(meter.getMeanRate()))
                            .build();
                } catch (Exception ex) {
                    logger.error("[UNEXPECTED_THINGS_HAPPENED] ke.metrics meter {}", e.getKey(), ex);
                    return null;
                }
            }).filter(Objects::nonNull).collect(toList());
        }

        public List<PointBuilder.Point> fromTimers(final Map<String, Timer> timers, final String measurement,
                final long timestamp, final TimeUnit timeUnit) {
            return timers.entrySet().stream().map(e -> {
                try {
                    Pair<String, Map<String, String>> nameTags = parseNameTags(e.getKey());
                    final Timer timer = e.getValue();
                    final Snapshot snapshot = timer.getSnapshot();

                    return new PointBuilder(measurement, timestamp, timeUnit).putTags(nameTags.getSecond())
                            .putField(filedName(nameTags.getFirst(), COUNT), snapshot.size())
                            .putField(filedName(nameTags.getFirst(), MIN), convertDuration(snapshot.getMin()))
                            .putField(filedName(nameTags.getFirst(), MAX), convertDuration(snapshot.getMax()))
                            .putField(filedName(nameTags.getFirst(), MEAN), convertDuration(snapshot.getMean()))
                            .putField(filedName(nameTags.getFirst(), STANDARD_DEVIATION),
                                    convertDuration(snapshot.getStdDev()))
                            .putField(filedName(nameTags.getFirst(), FIFTY_PERCENTILE),
                                    convertDuration(snapshot.getMedian()))
                            .putField(filedName(nameTags.getFirst(), SEVENTY_FIVE_PERCENTILE),
                                    convertDuration(snapshot.get75thPercentile()))
                            .putField(filedName(nameTags.getFirst(), NINETY_FIVE_PERCENTILE),
                                    convertDuration(snapshot.get95thPercentile()))
                            .putField(filedName(nameTags.getFirst(), NINETY_NINE_PERCENTILE),
                                    convertDuration(snapshot.get99thPercentile()))
                            .putField(filedName(nameTags.getFirst(), NINETY_NINE_POINT_NINE_PERCENTILE),
                                    convertDuration(snapshot.get999thPercentile()))
                            .putField(filedName(nameTags.getFirst(), ONE_MINUTE), convertRate(timer.getOneMinuteRate()))
                            .putField(filedName(nameTags.getFirst(), FIVE_MINUTE),
                                    convertRate(timer.getFiveMinuteRate()))
                            .putField(filedName(nameTags.getFirst(), FIFTEEN_MINUTE),
                                    convertRate(timer.getFifteenMinuteRate()))
                            .putField(filedName(nameTags.getFirst(), MEAN_MINUTE), convertRate(timer.getMeanRate()))
                            .putField(filedName(nameTags.getFirst(), RUN_COUNT), timer.getCount()).build();
                } catch (Exception ex) {
                    logger.error("[UNEXPECTED_THINGS_HAPPENED] ke.metrics timer {}", e.getKey(), ex);
                    return null;
                }
            }).filter(Objects::nonNull).collect(toList());
        }

        private <T, R> List<PointBuilder.Point> fromGaugesOrCounters(final Map<String, T> items,
                final Function<T, R> valueExtractor, final String measurement, final long timestamp,
                final TimeUnit timeUnit) {
            return items.entrySet().stream().map(e -> {
                try {
                    Pair<String, Map<String, String>> nameTags = parseNameTags(e.getKey());
                    final R value = valueExtractor.apply(e.getValue());
                    return new PointBuilder(measurement, timestamp, timeUnit).putTags(nameTags.getSecond())
                            .putField(nameTags.getFirst(), value).build();
                } catch (Exception ex) {
                    logger.error("[UNEXPECTED_THINGS_HAPPENED] ke.metrics gauge or counter {}", e.getKey(), ex);
                    return null;
                }
            }).filter(Objects::nonNull).collect(toList());
        }

        private Pair<String, Map<String, String>> parseNameTags(String metricName) {

            Preconditions.checkNotNull(metricName);

            String[] nameTags = metricName.split(":", 2);
            Map<String, String> tags = Arrays.asList(nameTags[1].split(",")).stream().map(t -> {
                String[] keyVal = t.split("=", 2);
                return new AbstractMap.SimpleEntry<>(keyVal[0], keyVal[1]);
            }).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
            return Pair.newPair(nameTags[0], tags);
        }

        private String filedName(String prefix, String suffix) {
            return String.join("_", prefix, suffix);
        }
    }

    private static final Set<Class> VALID_FIELD_CLASSES = ImmutableSet.of(Boolean.class, Byte.class, Character.class,
            Double.class, Float.class, Integer.class, Long.class, Short.class, String.class);

    public static class PointBuilder {

        // I definitely wanna a Measurement.Builder here,
        // but it couldn't be realized in a inner class.
        private final String measurement;
        private final long timestamp;
        private final TimeUnit timeUnit;
        private final Map<String, String> tags = new TreeMap<>();
        private final Map<String, Object> fields = new TreeMap<>();

        public PointBuilder(String measurement, long timestamp, TimeUnit timeUnit) {
            this.measurement = measurement;
            this.timestamp = timestamp;
            this.timeUnit = timeUnit;
        }

        private String handleCollection(final String key, final Collection collection) {
            for (final Object value : collection) {
                if (!isValidField(value)) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Measure collection field '%s' must contain only Strings and primitives: invalid field '%s'",
                            key, value));
                }
            }
            return collection.toString();
        }

        private <T> boolean isValidField(final T value) {
            return value == null || VALID_FIELD_CLASSES.contains(value.getClass());
        }

        private <T> Optional<T> handleField(final T value) {
            if (value instanceof Float) {
                final float f = (Float) value;
                if (!Float.isNaN(f) && !Float.isInfinite(f)) {
                    return Optional.of(value);
                }
            } else if (value instanceof Double) {
                final double d = (Double) value;
                if (!Double.isNaN(d) && !Double.isInfinite(d)) {
                    return Optional.of(value);
                }
            } else if (value instanceof Number) {
                return Optional.of(value);
            } else if (value instanceof String || value instanceof Character || value instanceof Boolean) {
                return Optional.of(value);
            }

            return Optional.empty();
        }

        public PointBuilder putTag(final String key, final String value) {
            tags.put(key, value);
            return this;
        }

        public PointBuilder putTags(final Map<String, String> items) {
            tags.putAll(items);
            return this;
        }

        public <T> PointBuilder putField(final String key, final T value) {

            if (value instanceof Collection<?>) {
                fields.put(key, handleCollection(key, (Collection) value));
            } else if (value != null) {
                handleField(value).ifPresent(s -> fields.put(key, s));
            }

            return this;
        }

        public <T> PointBuilder putFields(final Map<String, T> items) {
            items.forEach(this::putField);
            return this;
        }

        public PointBuilder.Point build() {
            if (this.fields.isEmpty()) {
                return null;
            }

            return new Point(measurement, tags, timestamp, timeUnit, fields);
        }

        @Getter
        @Setter
        @AllArgsConstructor
        public static class Point {
            private String measurement;
            private Map<String, String> tags;
            private Long time;
            private TimeUnit precision;
            private Map<String, Object> fields;

            public io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Point convert() {
                return io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Point.measurement(this.measurement)
                        .time(this.time, this.precision).tag(this.tags).fields(this.fields).build();
            }

            public String getUniqueKey() {
                StringBuilder builder = new StringBuilder();
                builder.append("Point [name=");
                builder.append(this.measurement);

                builder.append(", tags=");
                builder.append(this.tags);

                builder.append(", fields=");
                builder.append(this.fields.keySet());
                builder.append("]");
                return builder.toString();
            }
        }

    }
}
