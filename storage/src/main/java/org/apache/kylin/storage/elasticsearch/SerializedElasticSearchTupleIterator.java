package org.apache.kylin.storage.elasticsearch;


import static org.elasticsearch.index.query.QueryBuilders.*;

import java.io.IOException;
import java.util.*;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.ScanOutOfLimitException;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

/**
 * Created by liuze on 2016/1/13 0013.
 */
public class SerializedElasticSearchTupleIterator  implements ITupleIterator {


    private Stack<QueryBuilderUtil> stack = new Stack<QueryBuilderUtil>();

    private int scanCount;
    private StorageContext context;

    private Iterator<SearchHit> resultIterator;
    private TupleInfo tupleInfo;
    private Tuple tuple;
    private int scanCountDelta;

    private HashSet<TblColRef> dimensions;

    private QueryBuilder queryBuilder=null;
    private List<String> columnName;

    private int size=100000000;
    public SerializedElasticSearchTupleIterator(Client client,CubeInstance cube,Collection<TblColRef> dimensions,TupleFilter filter,StorageContext context){

        this.context=context;
        this.dimensions=(HashSet)dimensions;
        if(filter!=null) {
            buildFilter(filter);
        }
        String type="kylin";
        String index=cube.getName().toLowerCase();
        String [] fileds=new String[dimensions.size()];
        int i=0;
        for(TblColRef tcr:dimensions){
            fileds[i]=tcr.getName().toUpperCase();
            i++;
        }
        this.columnName=Arrays.asList(fileds);
        if(stack.size()!=0) {
            this.queryBuilder = stack.pop().queryBuilder;
        }else{
            this.queryBuilder = boolQuery();
        }
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .addFields(fileds)
                .setQuery(queryBuilder)
                .setSize(size)
                .execute().actionGet();

        doScan(response);

    }


    private final void doScan(SearchResponse response) {

        SearchHit[] searchHits=response.getHits().getHits();
        this.resultIterator= Arrays.asList(searchHits).iterator();
        this.tupleInfo = buildTupleInfo();
        this.tuple = new Tuple(this.tupleInfo);

    }

    private TupleInfo buildTupleInfo() {
        TupleInfo info = new TupleInfo();
        int index = 0;

        Iterator<TblColRef> iterator=dimensions.iterator();
        int i=0;
        while(iterator.hasNext()){
            TblColRef trf=iterator.next();
            info.setField(this.columnName.get(i), trf, trf.getType().getName(), index++);
            i++;
        }

        return info;
    }
    private void  buildFilter(TupleFilter filter) {

        if (filter.hasChildren()) {
            List<? extends TupleFilter> litf=filter.getChildren();
            for (TupleFilter f : litf) {
                if (f.getOperator()!= TupleFilter.FilterOperatorEnum.COLUMN && f.getOperator() != TupleFilter.FilterOperatorEnum.CONSTANT) {
                    buildFilter(f);
                }
            }

        }

        if(filter instanceof LogicalTupleFilter){

            LogicalTupleFilter cf=(LogicalTupleFilter)filter;

            BoolQueryBuilder bb=new BoolQueryBuilder();

            int level=filter.level+1;
            while(!stack.empty()) {

                if (stack.peek().level == level) {
                    QueryBuilder qb = stack.pop().queryBuilder;
                    if (cf.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
                        bb.must(qb);
                    }
                    if (cf.getOperator() == TupleFilter.FilterOperatorEnum.OR) {
                        bb.should(qb);
                    }
                }else{break;}
            }

            QueryBuilderUtil qbu=new QueryBuilderUtil(bb,filter.level);

            stack.push(qbu);


        }


        if(filter instanceof CompareTupleFilter){
            CompareTupleFilter cf=(CompareTupleFilter)filter;

            //TermQueryBuilder tqb=null;
            QueryBuilder qb=null;
            TupleFilter.FilterOperatorEnum op=cf.getOperator();
            String columnName=cf.getColumn().getName().toUpperCase();
            switch (op) {
                case EQ:
                    qb=termQuery(columnName, cf.getFirstValue());
                    break;
                case IN:
                    qb=new TermsQueryBuilder(columnName,cf.getValues());
                    break;
                case LT:
                    qb=rangeQuery(columnName).lt(cf.getFirstValue());
                    break;
                case LTE:
                    qb=rangeQuery(columnName).lte(cf.getFirstValue());
                    break;
                case GT:
                    qb=rangeQuery(columnName).gt(cf.getFirstValue());
                    break;
                case GTE:
                    qb=rangeQuery(columnName).gte(cf.getFirstValue());
                    break;
                case NEQ:
                case NOTIN:
                case ISNULL: // TODO ISNULL worth pass down as a special equal value
                case ISNOTNULL:
                    // let Optiq filter it!
                    break;
                default:
                    throw new UnsupportedOperationException(op.name());
            }

            QueryBuilderUtil qbdu=new QueryBuilderUtil(qb,filter.level);
            stack.push(qbdu);


        }

    }

    private void flushScanCountDelta() {
        context.increaseTotalScanCount(scanCountDelta);
        scanCountDelta = 0;
    }
    @Override
    public void remove() {

    }

    @Override
    public boolean hasNext() {
        // 1. check limit
        if (context.isLimitEnabled() && scanCount >= context.getLimit() + context.getOffset()) {
            return false;
        }
        // 3. check threshold
        if (scanCount >= context.getThreshold()) {
            throw new ScanOutOfLimitException("Scan row count exceeded threshold: " + context.getThreshold() + ", please add filter condition to narrow down backend scan range, like where clause.");
        }
        // 4. check cube segments
        return resultIterator.hasNext();
    }

    @Override
    public ITuple next() {

        // get next result from ES
        SearchHit result = null;
        if (resultIterator.hasNext()) {
            result = this.resultIterator.next();
            scanCount++;
            if (++scanCountDelta >= 1000)
                flushScanCountDelta();

        }
        if (result == null) {
            return null;
        }
        // translate result to tuple
        try {
            translateResult(result, this.tuple);
        } catch (IOException e) {
            throw new IllegalStateException("Can't translate result " + result, e);
        }
        return this.tuple;
    }

    @Override
    public void close() {

    }


    private void translateResult(SearchHit res, Tuple tuple) throws IOException {
        // groups


        for (Map.Entry<String, SearchHitField> e : res.getFields().entrySet()) {
            tuple.setDimensionValue(e.getKey(), e.getValue().getValues().get(0).toString());
        }


    }
}