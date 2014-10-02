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

import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;

import java.util.Arrays;

/**
 * @author xjiang
 */
public class FuzzyKeyEncoder extends RowKeyEncoder {

    public FuzzyKeyEncoder(CubeSegment seg, Cuboid cuboid) {
        super(seg, cuboid);
    }

    @Override
    protected byte[] defaultValue(int length) {
        byte[] keyBytes = new byte[length];
        Arrays.fill(keyBytes, RowConstants.FUZZY_MASK_ZERO);
        return keyBytes;
    }
}
