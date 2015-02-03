package com.kylinolap.dict;

import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by Hongbin Ma(Binmahone) on 12/17/14.
 */
public interface ISegment {

    public abstract int getColumnLength(TblColRef col);

    public abstract Dictionary<?> getDictionary(TblColRef col);
    
    public String getName();
    
    public String getUuid();
}
