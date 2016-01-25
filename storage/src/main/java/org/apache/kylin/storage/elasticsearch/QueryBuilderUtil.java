package org.apache.kylin.storage.elasticsearch;

import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by liuze on 2016/1/13 0013.
 */
public class QueryBuilderUtil {

    public QueryBuilder queryBuilder=null;
    public int level=0;

    public QueryBuilderUtil(QueryBuilder queryBuilder,int level){

        this.queryBuilder=queryBuilder;
        this.level=level;
    }
}
