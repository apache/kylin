package org.apache.kylin.metadata.realization;

import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

/**
 * Created by shaoshi on 10/30/15.
 */
public interface IRealizationSegment extends IBuildable {

    public String getUuid();
    
    public String getName();

    public String getStorageLocationIdentifier();
    
    public IRealization getRealization();
    
    public IJoinedFlatTableDesc getJoinedFlatTableDesc();
}
