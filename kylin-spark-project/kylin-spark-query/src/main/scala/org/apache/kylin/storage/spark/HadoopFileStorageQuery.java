package org.apache.kylin.storage.spark;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.RawQueryLastHacker;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest;
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


    public GTCubeStorageQueryRequest getStorageQueryRequest(StorageContext context, SQLDigest sqlDigest,
                                                            TupleInfo returnTupleInfo) {
        context.setStorageQuery(this);

        //cope with queries with no aggregations
        RawQueryLastHacker.hackNoAggregations(sqlDigest, cubeDesc, returnTupleInfo);

        // Customized measure taking effect: e.g. allow custom measures to help raw queries
        notifyBeforeStorageQuery(sqlDigest);

        Collection<TblColRef> groups = sqlDigest.groupbyColumns;
        TupleFilter filter = sqlDigest.filter;

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<>();
        Set<FunctionDesc> metrics = new LinkedHashSet<>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        // all dimensions = groups + other(like filter) dimensions
        Set<TblColRef> otherDims = Sets.newHashSet(dimensions);
        otherDims.removeAll(groups);

        // expand derived (xxxD means contains host columns only, derived columns were translated)
        Set<TblColRef> derivedPostAggregation = Sets.newHashSet();
        Set<TblColRef> groupsD = expandDerived(groups, derivedPostAggregation);
        Set<TblColRef> otherDimsD = expandDerived(otherDims, derivedPostAggregation);
        otherDimsD.removeAll(groupsD);

        // identify cuboid
        Set<TblColRef> dimensionsD = new LinkedHashSet<>();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(otherDimsD);
        Cuboid cuboid = findCuboid(cubeInstance, dimensionsD, metrics);
        context.setCuboid(cuboid);
        return new GTCubeStorageQueryRequest(cuboid, dimensionsD, groupsD, null, null, null,
                metrics, null, null, null, context);
    }
}
