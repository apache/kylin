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

package org.apache.kylin.dimension;

import static org.junit.Assert.fail;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.DateFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Note the test must be consistent with TimeStrDictionaryTest,
 * to ensure TimeDimEnc is backward compatible with TimeStrDictionary.
 */
@Ignore("Unused now")
public class TimeDimEncTest {
    TimeDimEnc enc;
    byte[] buf;

    @Before
    public void setup() {
        enc = new TimeDimEnc();
        buf = new byte[enc.getLengthOfEncoding()];
    }

    private long encode(String value) {
        enc.encode(value, buf, 0);
        return BytesUtil.readLong(buf, 0, buf.length);
    }

    private String decode(long code) {
        BytesUtil.writeLong(code, buf, 0, buf.length);
        return enc.decode(buf, 0, buf.length);
    }

    @Test
    public void basicTest() {
        long a = encode("1999-01-01");
        long b = encode("1999-01-01 00:00:00");
        long c = encode("1999-01-01 00:00:00.000");
        long d = encode("1999-01-01 00:00:00.022");

        Assert.assertEquals(a, b);
        Assert.assertEquals(a, c);
        Assert.assertEquals(a, d);
    }

    @Test
    public void testEncodeDecode() {
        encodeDecode("1999-01-12");
        encodeDecode("2038-01-09");
        encodeDecode("2038-01-08");
        encodeDecode("1970-01-01");
        encodeDecode("1970-01-02");

        encodeDecode("1999-01-12 11:00:01");
        encodeDecode("2038-01-09 01:01:02");
        encodeDecode("2038-01-19 03:14:06");
        encodeDecode("1970-01-01 23:22:11");
        encodeDecode("1970-01-02 23:22:11");
    }

    @Test
    public void testIllegal() {
        try {
            encode("10000-1-1");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // good
        }
    }

    public void encodeDecode(String origin) {
        long a = encode(origin);
        String back = decode(a);

        String originChoppingMilis = DateFormat.formatToTimeWithoutMilliStr(DateFormat.stringToMillis(origin));
        String backMillis = DateFormat.formatToTimeWithoutMilliStr(Long.parseLong(back));
        Assert.assertEquals(originChoppingMilis, backMillis);
    }

}
