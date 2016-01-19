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

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;

import java.util.Arrays;
import java.util.List;

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


    //for non-sharding cases it will only return one byte[] with not shard at beginning
    public List<byte[]> getRowKeysDifferentShards(byte[] halfCookedKey) {
        final short cuboidShardNum = cubeSeg.getCuboidShardNum(cuboid.getId());

        if (!enableSharding) {
            return Lists.newArrayList(halfCookedKey);//not shard to append at head, so it is already well cooked
        } else {
            List<byte[]> ret = Lists.newArrayList();
            for (short i = 0; i < cuboidShardNum; ++i) {
                short shard = ShardingHash.normalize(cubeSeg.getCuboidBaseShard(cuboid.getId()), i, cubeSeg.getTotalShards());
                byte[] cookedKey = Arrays.copyOf(halfCookedKey, halfCookedKey.length);
                BytesUtil.writeShort(shard, cookedKey, 0, RowConstants.ROWKEY_SHARDID_LEN);
                ret.add(cookedKey);
            }
            return ret;
        }

    }
}
