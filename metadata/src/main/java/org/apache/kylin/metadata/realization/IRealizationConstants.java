package org.apache.kylin.metadata.realization;

/**
 */
public class IRealizationConstants {

    public final static String SharedHbaseStorageLocationPrefix = "KYLIN_";
    public final static String CubeHbaseStorageLocationPrefix = "KYLIN_";
    public final static String IIHbaseStorageLocationPrefix = "KYLIN_II_";

    /**
     * For each cube htable, we leverage htable's metadata to keep track of
     * which kylin server(represented by its kylin_metadata prefix) owns this htable
     */
    public final static String HTableTag = "KYLIN_HOST";

}
