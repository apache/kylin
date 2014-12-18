package com.kylinolap.metadata.realization;

import com.kylinolap.common.persistence.RootPersistentEntity;

/**
 * Created by Hongbin Ma(Binmahone) on 12/17/14.
 */
public abstract class AbstractRealization extends RootPersistentEntity implements IRealization {

    public String getCanonicalName(String name) {
        return getType() + "[name=" + name + "]";
    }
}
