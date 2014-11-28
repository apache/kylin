package com.kylinolap.metadata.model.realization;

import java.util.Set;

public interface IDataModelRealization {

    /** Return a cost indicator for the realization, the lower, the better. Range from 0 - 100 typically. */
    int getCost();
    
    Set<TblColRef> getSupportedColumns();
}
