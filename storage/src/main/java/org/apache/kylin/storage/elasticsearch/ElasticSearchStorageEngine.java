

package org.apache.kylin.storage.elasticsearch;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

import java.util.*;

import org.apache.kylin.common.persistence.ElasticSearchClient;

import org.apache.kylin.cube.CubeInstance;

import org.apache.kylin.cube.model.CubeDesc;

import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * Created by liuze on 2016/1/8 0008.
 */


public class ElasticSearchStorageEngine implements IStorageEngine {




    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchStorageEngine.class);

    private static final int MERGE_KEYRANGE_THRESHOLD = 100;
    private static final long MEM_BUDGET_PER_QUERY = 3L * 1024 * 1024 * 1024; // 3G

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;

    QueryBuilder qb = boolQuery();

    public ElasticSearchStorageEngine(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest) {

        TupleFilter filter = sqlDigest.filter;
        if(filter!=null) {
            setLevel(filter, 1);
        }
        // build dimension & metrics
        Collection<TblColRef> dimension = new HashSet<TblColRef>();    //all dim colums except metrics
        Collection<FunctionDesc> metrics = new HashSet<FunctionDesc>();    //all func Desc
        buildDimensionsAndMetrics(dimension, metrics, sqlDigest);
        Collection<TblColRef> dimensions=sqlDigest.allColumns;
        //Collection<TblColRef> groupbyColumns=sqlDigest.groupbyColumns;


        //Collection<FunctionDesc> aggregations=sqlDigest.aggregations;


        //setThreshold(dimensionsD, valueDecoders, context); // set cautious threshold to prevent out of memory
        setLimit(filter, context);

        Client client = ElasticSearchClient.get(context.getEsClusterUrl());
        return new SerializedElasticSearchTupleIterator(client, cubeInstance, dimensions, filter, context);
    }

    private void buildDimensionsAndMetrics(Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics, SQLDigest sqlDigest) {

        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric()) {
                metrics.add(func);
            }
        }

        for (TblColRef column : sqlDigest.allColumns) {
            // skip measure columns
            if (sqlDigest.metricColumns.contains(column)) {
                continue;
            }
            dimensions.add(column);
        }
    }

    private void  setLevel(TupleFilter filter, int j) {

        if(filter==null){
            filter=new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        }
        filter.level=j;

        if (filter.hasChildren()) {
            j=j+1;
            List<? extends TupleFilter>  litf=filter.getChildren();
            for (TupleFilter f : litf) {
                if (f.getOperator().getValue() != 30 && f.getOperator().getValue() != 31) {
                    setLevel(f,j);
                }

            }

        }
    }









    private void setLimit(TupleFilter filter, StorageContext context) {
        boolean goodAggr = context.isExactAggregation();
        boolean goodFilter = filter == null || (TupleFilter.isEvaluableRecursively(filter) && context.isCoprocessorEnabled());
        boolean goodSort = context.hasSort() == false;
        if (goodAggr && goodFilter && goodSort) {
            logger.info("Enable limit " + context.getLimit());
            context.enableLimit();
        }
    }


}


