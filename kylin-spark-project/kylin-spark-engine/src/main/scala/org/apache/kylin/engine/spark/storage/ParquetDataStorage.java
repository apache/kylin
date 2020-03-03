package org.apache.kylin.engine.spark.storage;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;

public class ParquetDataStorage implements IStorage {
    @Override
    public IStorageQuery createQuery(IRealization realization) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        Class clz;
        try {
            clz = Class.forName("org.apache.kylin.engine.spark.NSparkCubingEngine$NSparkCubingStorage");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (engineInterface == clz) {
            return (I) ClassUtil.newInstance("org.apache.kylin.engine.spark.storage.ParquetStorage");
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }

}