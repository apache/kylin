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

package org.apache.kylin.stream.source.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.stream.core.source.MessageParserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Created by guoning on 2019-04-29.
 */
public class LongTimeParser extends AbstractTimeParser {

    private static final Logger logger = LoggerFactory.getLogger(LongTimeParser.class);
    private String tsPattern = null;

    public LongTimeParser(MessageParserInfo parserInfo) {
        super(parserInfo);
        tsPattern = parserInfo.getTsPattern().toUpperCase(Locale.ENGLISH);
    }

    /**
     * Parse a string time to a long value (epoch time)
     *
     * @param time
     * @return
     */
    public long parseTime(String time) throws IllegalArgumentException {
        long t;
        if (StringUtils.isEmpty(time)) {
            t = 0;
        } else {
            try {
                t = Long.parseLong(time);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(e);
            }
        }
        if ("S".equals(tsPattern)) {
            t = t * 1000;
        }
        return t;
    }
}
