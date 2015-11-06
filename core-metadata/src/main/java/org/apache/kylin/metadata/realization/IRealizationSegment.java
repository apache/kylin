package org.apache.kylin.metadata.realization;

import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

/**
 */
public interface IRealizationSegment extends IBuildable {

    public String getUuid();
    
    public String getName();

    public String getStorageLocationIdentifier();
    
    public IRealization getRealization();
    
    public IJoinedFlatTableDesc getJoinedFlatTableDesc();
}
