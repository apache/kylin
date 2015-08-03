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

package org.apache.kylin.metadata.measure;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;

/**
 * @author yangli9
 * 
 */
public class BigDecimalSerializer extends MeasureSerializer<BigDecimal> {

    @Override
    public void serialize(BigDecimal value, ByteBuffer out) {
        byte[] bytes = value.unscaledValue().toByteArray();

        BytesUtil.writeVInt(value.scale(), out);
        BytesUtil.writeVInt(bytes.length, out);
        out.put(bytes);
    }

    @Override
    public BigDecimal deserialize(ByteBuffer in) {
        int scale = BytesUtil.readVInt(in);
        int n = BytesUtil.readVInt(in);

        byte[] bytes = new byte[n];
        in.get(bytes);

        return new BigDecimal(new BigInteger(bytes), scale);
    }

    @Override
    public BigDecimal valueOf(byte[] value) {
        if (value == null)
            return new BigDecimal(0);
        else
            return new BigDecimal(Bytes.toString(value));
    }

}
