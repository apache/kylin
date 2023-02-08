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

package org.apache.kylin.parser.utils;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kylin.parser.AbstractDataParser;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParserBenchMark {

    private static final long DATA_SIZE_20K = 20_000L;
    private static final long DATA_SIZE_40K = 40_000L;
    private static final long DATA_SIZE_60K = 60_000L;

    private ParserBenchMark() {
    }

    public static <I> Long test20K(I input, AbstractDataParser<I> parser) {
        return testParse(input, parser, DATA_SIZE_20K);
    }

    public static <I> Long test40K(I input, AbstractDataParser<I> parser) {
        return testParse(input, parser, DATA_SIZE_40K);
    }

    public static <I> Long test60K(I input, AbstractDataParser<I> parser) {
        return testParse(input, parser, DATA_SIZE_60K);
    }

    public static <I> Long testWithSize(I input, AbstractDataParser<I> parser, long size) {
        checkSize(size);
        return testParse(input, parser, size);
    }

    private static <I> Long testParse(I input, AbstractDataParser<I> parser, long size) {
        StopWatch stopWatch = new StopWatch("Parse Testing");
        stopWatch.start();
        for (int i = 0; i < size; i++) {
            try {
                parser.process(input);
            } catch (Exception e) {
                log.error("Abnormal data during test", e);
            }
        }
        stopWatch.stop();
        return stopWatch.getTime(TimeUnit.MILLISECONDS);
    }

    private static void checkSize(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException("The number of test data size should be > 0");
        }
    }
}
