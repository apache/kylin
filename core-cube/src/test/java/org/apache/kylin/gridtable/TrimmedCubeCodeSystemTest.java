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

package org.apache.kylin.gridtable;

import static org.apache.kylin.cube.gridtable.TrimmedCubeCodeSystem.readDimensionEncoding;
import static org.apache.kylin.cube.gridtable.TrimmedCubeCodeSystem.writeDimensionEncoding;

import java.nio.ByteBuffer;

import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.FixedLenHexDimEnc;
import org.junit.Assert;
import org.junit.Test;

public class TrimmedCubeCodeSystemTest {
    @Test
    public void testFixLenHexEncSerDser() {
        FixedLenHexDimEnc enc = new FixedLenHexDimEnc(6);
        ByteBuffer buff = ByteBuffer.allocate(1024);
        writeDimensionEncoding(enc, buff);
        buff.flip();
        DimensionEncoding dimensionEncoding = readDimensionEncoding(buff);
        Assert.assertEquals(3, dimensionEncoding.asDataTypeSerializer().peekLength(null));
    }
}
