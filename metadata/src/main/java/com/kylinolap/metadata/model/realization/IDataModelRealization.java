package com.kylinolap.metadata.model.realization;

public interface IDataModelRealization {

    /** Return a cost indicator for the realization, the lower, the better. Range from 0 - 100 typically. */
    int getCost();
    
    
}
