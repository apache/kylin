/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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

package org.apache.kylin.engine.spark.metadata.cube.datatype;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author yangli9
 * 
 */
public class BigDecimalSerializer extends DataTypeSerializer<BigDecimal> {

    private static final Logger logger = LoggerFactory.getLogger(BigDecimalSerializer.class);

    final DataType type;
    final int maxLength;

    transient int avoidVerbose = 0;

    public BigDecimalSerializer(DataType type) {
        this.type = type;
        // see serialize(): 1 byte scale, 1 byte length, assume every 2 digits takes 1 byte
        this.maxLength = 1 + 1 + (type.getPrecision() + 1) / 2;
    }

    @Override
    public void serialize(BigDecimal value, ByteBuffer out) {
        if (value.scale() > type.getScale()) {
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn("value's scale has exceeded the " + type.getScale() + ", cut it off, to ensure encoded value do not exceed maxLength " + maxLength + " times:" + (avoidVerbose));
            }
            value = value.setScale(type.getScale(), BigDecimal.ROUND_HALF_EVEN);
        }
        byte[] bytes = value.unscaledValue().toByteArray();
        if (bytes.length + 2 > maxLength) {
            throw new IllegalArgumentException("'" + value + "' exceeds the expected length for type " + type);
        }

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
    public int peekLength(ByteBuffer in) {
        int mark = in.position();

        @SuppressWarnings("unused")
        int scale = BytesUtil.readVInt(in);
        int n = BytesUtil.readVInt(in);
        int len = in.position() - mark + n;

        in.position(mark);
        return len;
    }

    @Override
    public int maxLength() {
        return maxLength;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 8;
    }

    @Override
    public BigDecimal valueOf(String str) {
        return new BigDecimal(str);
    }

}
