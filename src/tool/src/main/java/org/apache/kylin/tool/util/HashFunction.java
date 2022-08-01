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
package org.apache.kylin.tool.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum HashFunction {
    MD5("MD5"), SHA1("SHA1"), SHA256("SHA-256"), SHA512("SHA-512");

    private static final Logger logger = LoggerFactory.getLogger(HashFunction.class);
    private String name;

    HashFunction(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public byte[] checksum(File input) {
        try (FileInputStream inputStream = new FileInputStream(input)) {
            return checksum(inputStream);
        } catch (Exception e) {
            logger.error("Failed to checksum and return null: input={}", input.getAbsolutePath(), e);
        }
        return new byte[] { 0x01, 0x01, 0x01 };
    }

    public byte[] checksum(InputStream in) throws IOException {
        try {
            MessageDigest digester = MessageDigest.getInstance(this.getName());
            byte[] block = new byte[4096];
            int length;
            while ((length = in.read(block)) > 0) {
                digester.update(block, 0, length);
            }

            return digester.digest();
        } catch (Exception e) {
            logger.error("Failed to checksum and return null.", e);
        }
        return new byte[] { 0x01, 0x01, 0x01 };
    }

}
