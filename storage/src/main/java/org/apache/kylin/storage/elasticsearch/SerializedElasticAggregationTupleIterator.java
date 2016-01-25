package org.apache.kylin.storage.elasticsearch;


import static org.elasticsearch.index.query.QueryBuilders.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.ScanOutOfLimitException;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

/**
 * Created by liuze on 2016/1/19 0013.
 */
public class SerializedElasticAggregationTupleIterator implements ITupleIterator {


    private Stack<QueryBuilderUtil> stack = new Stack<QueryBuilderUtil>();

    private TermsBuilder termsBuilder=null;

    private QueryBuilder queryBuilder=null;

    private int scanCount;
    private StorageContext context;

    private Iterator<Map<String,Object>> resultIterator;
    private TupleInfo tupleInfo;
    private Tuple tuple;
    private int scanCountDelta;

    private HashSet<TblColRef> groupbyColumns;
    private List<FunctionDesc> aggregations;
    private List<String> columnName;


    private List<Map<String,Object>> allRow=new ArrayList<>();
    private List<String> metric=null;

    private int size=100000000;

    public SerializedElasticAggregationTupleIterator(Client client, CubeInstance cube,  TupleFilter filter, Collection<TblColRef> groupbyColumns, Collection<FunctionDesc> aggregations, StorageContext context){

        this.context=context;
        this.aggregations=(List)aggregations;
        this.groupbyColumns=(HashSet)groupbyColumns;
        if(filter!=null) {
            buildFilter(filter);
        }

        String type="kylin";
        String index=cube.getName().toLowerCase();
        String [] fileds=new String[groupbyColumns.size()];
        int i=0;
        for(TblColRef tcr:groupbyColumns){
            fileds[i]=tcr.getName().toUpperCase();
            i++;
        }

        this.columnName=Arrays.asList(fileds);
        if(stack.size()!=0) {
            this.queryBuilder = stack.pop().queryBuilder;
        }else{
            this.queryBuilder = boolQuery();
        }



        buildAggregation();

        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.COUNT)
                .setQuery(queryBuilder)
                .addAggregation(termsBuilder)
                .setSize(size)
                .execute().actionGet();
        System.out.println(queryBuilder);

        Map<String,Object> allCo =new HashMap<>();
        doScan(response.getAggregations(),allCo);


    }


    private final void doScan(Aggregations aggregations,Map<String,Object> allCo) {

        Map<String, Aggregation> aggMap = aggregations.asMap();

        String colName = null;
        Terms gradeTerms =null;
        for(int i=0;i<columnName.size();i++){

            colName = columnName.get(i).toUpperCase();
            gradeTerms = (Terms) aggMap.get(colName);
            if (gradeTerms!=null){
                break;
            }
        }

        if (gradeTerms != null) {


            List<Terms.Bucket> grade = gradeTerms.getBuckets();

            for (int i = 0; i < grade.size(); i++) {

                allCo.put(colName.toUpperCase(), grade.get(i).getKey());

                for(String mtr:metric) {
                    Object sub = grade.get(i).getAggregations().get(mtr);
                    Method getvalueMethod = null;
                    if (sub != null) {
                        try {
                            getvalueMethod = sub.getClass().getDeclaredMethod("getValue");
                            Object sumA = getvalueMethod.invoke(sub);
                            allCo.put(mtr.toUpperCase(), sumA);

                        } catch (NoSuchMethodException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }

                doScan(grade.get(i).getAggregations(), allCo);

            }

        } else {

            Map<String,Object> re=new HashMap<>();
            String row="";
            for(Map.Entry<String, Object> entry:allCo.entrySet()){
                row=row+"|"+entry.getKey()+":"+entry.getValue();
                re.put(entry.getKey(),entry.getValue());
            }
            allRow.add(re);
        }

        this.resultIterator=allRow.iterator();
        this.tupleInfo = buildTupleInfo();
        this.tuple = new Tuple(this.tupleInfo);

    }

    private void buildAggregation() {

        metric=new ArrayList<>();
        int j=0;
        for(TblColRef trf:this.groupbyColumns){
            String groupName=trf.getName().toUpperCase();
            if(j==0){
                this.termsBuilder= AggregationBuilders.terms(groupName).field(groupName).size(size);
                for(FunctionDesc fcd:this.aggregations){
                    String parameter=fcd.getParameter().getValue();
                    metric.add(parameter);

                    if (fcd.isMax()) {
                        termsBuilder = termsBuilder.subAggregation(AggregationBuilders.max(parameter).field(parameter));
                    }
                    if (fcd.isMin()) {
                        termsBuilder = termsBuilder.subAggregation(AggregationBuilders.min(parameter).field(parameter));
                    }
                    if (fcd.isSum()) {
                        termsBuilder = termsBuilder.subAggregation(AggregationBuilders.sum(parameter).field(parameter));
                    }
                    if (fcd.isCount()) {
                        termsBuilder = termsBuilder.subAggregation(AggregationBuilders.count(parameter).field(parameter));
                    }
                    if (fcd.isCountDistinct()) {
                        termsBuilder = termsBuilder.subAggregation(AggregationBuilders.cardinality(parameter).field(parameter));
                    }

                }
            }else{
                termsBuilder=AggregationBuilders.terms(groupName).field(groupName).size(size).subAggregation(termsBuilder).size(size);
            }
            j++;
        }
    }
    private TupleInfo buildTupleInfo() {
        TupleInfo info = new TupleInfo();
        int index = 0;

        int i=0;
        Iterator<TblColRef> iterator=groupbyColumns.iterator();
        while(iterator.hasNext()){
            TblColRef trf=iterator.next();
            info.setField(this.columnName.get(i), trf, trf.getType().getName(), index++);
            i++;
        }

        for(FunctionDesc fcd:this.aggregations){
            info.setField(fcd.getParameter().getValue().toUpperCase(),null,fcd.getParameter().getType(),index++);
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
        Map<String,Object> result = null;
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


    private void translateResult(Map<String,Object> res, Tuple tuple) throws IOException {

        for (Map.Entry<String, Object> e : res.entrySet()) {
            String key=e.getKey().toUpperCase();
            Object value=e.getValue();

            if(this.metric.contains(e.getKey())) {
                String dataType=null;
                for(FunctionDesc fd:this.aggregations){
                    if(fd.getParameter().getValue().equals(key)){
                        dataType=fd.getParameter().getColRefs().get(0).getDatatype();
                    }
                }
                if(value instanceof Double){
                    if("bigint".equalsIgnoreCase(dataType)){
                        long l = new Double(value.toString()).longValue();
                        tuple.setMeasureValue(key.toUpperCase(), l);

                    }else{
                        tuple.setMeasureValue(key.toUpperCase(), value);
                    }
                }
                if(value instanceof Long){
                    tuple.setMeasureValue(key.toUpperCase(), value);
                }
            }else {

                tuple.setDimensionValue(key.toUpperCase(), value.toString());

            }
        }


    }
}
