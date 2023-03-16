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
package org.apache.kylin.rest.security;

import static org.springframework.security.crypto.bcrypt.BCrypt.hashpw;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kylin.common.util.Pair;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.cache.CacheLoader;
import org.apache.kylin.guava30.shaded.common.cache.LoadingCache;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CachedBCryptPasswordEncoder extends BCryptPasswordEncoder {
    private final Pattern BCRYPT_PATTERN = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");

    static boolean equalsNoEarlyReturn(String a, String b) {
        char[] caa = a.toCharArray();
        char[] cab = b.toCharArray();

        if (caa.length != cab.length) {
            return false;
        }

        byte ret = 0;
        for (int i = 0; i < caa.length; i++) {
            ret |= caa[i] ^ cab[i];
        }
        return ret == 0;
    }

    private final LoadingCache<Pair<String, String>, String> cached;

    public CachedBCryptPasswordEncoder() {
        this.cached = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(15, TimeUnit.MINUTES)
                .build(new CacheLoader<Pair<String, String>, String>() {
                    @Override
                    public String load(Pair<String, String> key) {
                        String plaintext = key.getFirst();
                        String hashed = key.getSecond();
                        return hashpw(plaintext, hashed);
                    }
                });
    }

    @SneakyThrows
    @Override
    public boolean matches(CharSequence rawPassword, String encodedPassword) {
        if (rawPassword == null) {
            throw new IllegalArgumentException("rawPassword cannot be null");
        }

        if (encodedPassword == null || encodedPassword.length() == 0) {
            log.warn("Empty encoded password");
            return false;
        }

        if (!BCRYPT_PATTERN.matcher(encodedPassword).matches()) {
            log.warn("Encoded password does not look like BCrypt");
            return false;
        }
        return equalsNoEarlyReturn(encodedPassword, cached.get(Pair.newPair(rawPassword.toString(), encodedPassword)));
    }
}
