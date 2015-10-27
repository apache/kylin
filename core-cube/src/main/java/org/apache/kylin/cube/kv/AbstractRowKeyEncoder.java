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

package org.apache.kylin.cube.kv;

import java.util.Map;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author xjiang
 * 
 */
public abstract class AbstractRowKeyEncoder {

    public static final byte DEFAULT_BLANK_BYTE = Dictionary.NULL;

    protected static final Logger logger = LoggerFactory.getLogger(AbstractRowKeyEncoder.class);

    public static AbstractRowKeyEncoder createInstance(CubeSegment cubeSeg, Cuboid cuboid) {
        return new RowKeyEncoder(cubeSeg, cuboid);
    }

    protected final Cuboid cuboid;
    protected byte blankByte = DEFAULT_BLANK_BYTE;
    protected boolean encodeShard = true;

    protected AbstractRowKeyEncoder(Cuboid cuboid) {
        this.cuboid = cuboid;
    }

    public void setBlankByte(byte blankByte) {
        this.blankByte = blankByte;
    }

    public void setEncodeShard(boolean encodeShard) {
        this.encodeShard = encodeShard;
    }

    abstract public byte[] encode(Map<TblColRef, String> valueMap);

    abstract public byte[] encode(byte[][] values);
}
