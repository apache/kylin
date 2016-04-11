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

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;

/**
 * A LazyRowKeyEncoder will not try to calculate shard
 * It works for both enableSharding or non-enableSharding scenario
 * Usually it's for sharded cube scanning, later all possible shard will be rewrite
 */
public class LazyRowKeyEncoder extends RowKeyEncoder {
    public LazyRowKeyEncoder(CubeSegment cubeSeg, Cuboid cuboid) {
        super(cubeSeg, cuboid);
    }

    protected short calculateShard(byte[] key) {
        if (enableSharding) {
            return 0;
        } else {
            throw new RuntimeException("If enableSharding false, you should never calculate shard");
        }
    }

}
