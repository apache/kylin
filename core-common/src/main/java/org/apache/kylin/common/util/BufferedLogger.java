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

package org.apache.kylin.common.util;

import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

/**
 * A Logger that remembers all the logged message.
 *
 * <b>This class is not thread-safe.</b>
 */
public class BufferedLogger implements Logger {
    private final org.slf4j.Logger wrappedLogger;
    private final StringBuilder buffer = new StringBuilder();

    private static int MAX_BUFFER_SIZE = 10 * 1024 * 1024;

    public BufferedLogger(org.slf4j.Logger wrappedLogger) {
        this.wrappedLogger = wrappedLogger;
    }

    @Override
    public void log(String message) {
        wrappedLogger.info(message);
        if (buffer.length() < MAX_BUFFER_SIZE) {
            buffer.append(message).append("\n");
        }
    }

    @Override
    public void log(String message, Object... arguments) {
        FormattingTuple ft = MessageFormatter.arrayFormat(message, arguments);
        log(ft.getMessage());
    }

    public String getBufferedLog() {
        return buffer.toString();
    }

    public void resetBuffer() {
        buffer.setLength(0);
    }
}
