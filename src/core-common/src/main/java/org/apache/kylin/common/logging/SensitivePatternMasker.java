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

package org.apache.kylin.common.logging;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.util.PerformanceSensitive;

@Plugin(name = "SensitivePatternMasker", category = "Converter")
@ConverterKeys({ "mask" })
public class SensitivePatternMasker extends LogEventPatternConverter {
    private static final String PREFIX_GROUP_NAME = "prefix";
    private static final String SENSITIVE_GROUP_NAME = "sensitive";
    private static final String MASK = "******";
    private static final Pattern SENSITIVE_PATTERN = Pattern.compile(String.format(Locale.ROOT,
            "(?<%s>password\\s*[:=])(?<%s>[^,.!]*)", PREFIX_GROUP_NAME, SENSITIVE_GROUP_NAME),
            Pattern.CASE_INSENSITIVE);

    public static SensitivePatternMasker newInstance(final Configuration config, final String[] options) {
        return new SensitivePatternMasker();
    }

    protected SensitivePatternMasker() {
        super("mask", "mask");
    }

    private String mask(String message) {
        Matcher matcher = SENSITIVE_PATTERN.matcher(message);
        if (matcher.find()) {
            return matcher.replaceAll(String.format(Locale.ROOT, "${%s}%s", PREFIX_GROUP_NAME, MASK));
        }
        return message;
    }

    @PerformanceSensitive({ "allocation" })
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        toAppendTo.append(mask(event.getMessage().getFormattedMessage()));
    }
}
