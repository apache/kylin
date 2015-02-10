package com.kylinolap.metadata.realization;

public interface IRealizationProvider {

    RealizationType getRealizationType();
    
    IRealization getRealization(String name);
}
