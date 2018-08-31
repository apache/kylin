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

package org.apache.kylin.common.persistence;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class BrokenInputStream extends InputStream {
    private static Logger logger = LoggerFactory.getLogger(BrokenInputStream.class);
    private final ByteArrayInputStream in;

    public BrokenInputStream(BrokenEntity brokenEntity) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            IOUtils.write(BrokenEntity.MAGIC, out);
            IOUtils.write(JsonUtil.writeValueAsBytes(brokenEntity), out);
        } catch (IOException e) {
            logger.error("There is something error when we serialize BrokenEntity: ", e);
            throw new RuntimeException("There is something error when we serialize BrokenEntity.");
        }

        in = new ByteArrayInputStream(out.toByteArray());
    }

    @Override
    public int read() {
        return in.read();
    }

    @Override
    public void close() throws IOException {
        in.close();
        super.close();
    }
}
