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

import java.io.OutputStream;

import org.slf4j.Logger;

public class LogOutputStream extends OutputStream {
    private final Logger logger;

    /**
     * The internal memory for the written bytes.
     */
    private StringBuilder mem;

    public LogOutputStream(final Logger logger) {
        this.logger = logger;
        mem = new StringBuilder();
    }

    @Override
    public void write(final int b) {
        if ((char) b == '\n') {
            flush();
            return;
        }
        mem.append((char) b);
    }

    @Override
    public void flush() {
        logger.info(mem.toString());
        mem = new StringBuilder();
    }

}
