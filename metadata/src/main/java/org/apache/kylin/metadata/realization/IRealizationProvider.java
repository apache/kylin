package org.apache.kylin.metadata.realization;

public interface IRealizationProvider {

    RealizationType getRealizationType();
    
    IRealization getRealization(String name);
}
