/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.cube.kv;

import java.util.Arrays;

import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.metadata.model.realization.TblColRef;

/**
 * 
 * @author xjiang
 * 
 */
public class FuzzyMaskEncoder extends RowKeyEncoder {

    public FuzzyMaskEncoder(CubeSegment seg, Cuboid cuboid) {
        super(seg, cuboid);
    }

    @Override
    protected int fillHeader(byte[] bytes, byte[][] values) {
        // always fuzzy match cuboid ID to lock on the selected cuboid
        int cuboidStart = this.headerLength - RowConstants.ROWKEY_CUBOIDID_LEN;
        Arrays.fill(bytes, 0, cuboidStart, RowConstants.FUZZY_MASK_ONE);
        Arrays.fill(bytes, cuboidStart, this.headerLength, RowConstants.FUZZY_MASK_ZERO);
        return this.headerLength;
    }

    @Override
    protected void fillColumnValue(TblColRef column, int columnLen, byte[] value, int valueLen, byte[] outputValue, int outputValueOffset) {
        if (value == null) {
            Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, RowConstants.FUZZY_MASK_ONE);
        } else {
            Arrays.fill(outputValue, outputValueOffset, outputValueOffset + columnLen, RowConstants.FUZZY_MASK_ZERO);
        }
    }
}
