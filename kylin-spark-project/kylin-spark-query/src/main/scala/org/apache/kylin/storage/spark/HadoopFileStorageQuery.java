package org.apache.kylin.storage.spark;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HadoopFileStorageQuery extends GTCubeStorageQueryBase {
    private static final Logger log = LoggerFactory.getLogger(HadoopFileStorageQuery.class);

    public HadoopFileStorageQuery(CubeInstance cube) {
        super(cube);
    }

    @Override
    protected String getGTStorage() {
        throw new UnsupportedOperationException("Unsupported getGTStorage.");
    }
}
