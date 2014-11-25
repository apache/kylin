package com.kylinolap.cube.invertedindex;

import java.util.List;

import com.google.common.collect.Lists;

public class ShardingSliceBuilder {

    SliceBuilder[] builders;
    
    public ShardingSliceBuilder(TableRecordInfo info) {
        int sharding = info.getDescriptor().getSharding();
        builders = new SliceBuilder[sharding];
        for (short i = 0; i < sharding; i++) {
            builders[i] = new SliceBuilder(info, i);
        }
    }
    
    // NOTE: record must be appended in time order
    public Slice append(TableRecord rec) {
        short shard = rec.getShard();
        return builders[shard].append(rec);
    }
    
    public List<Slice> close() {
        List<Slice> result = Lists.newArrayList();
        for (SliceBuilder builder : builders) {
            Slice slice = builder.close();
            if (slice != null)
                result.add(slice);
        }
        return result;
    }

}
