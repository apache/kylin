package org.apache.kylin.storage.test;

/**
 * Created by liuze on 2016/1/14 0014.
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.junit.Assert.assertTrue;

import java.util.*;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.model.*;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.elasticsearch.ElasticSearchStorageEngine;
import org.apache.kylin.storage.elasticsearch.QueryBuilderUtil;
import org.apache.kylin.storage.hbase.ScanOutOfLimitException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.junit.*;

public class EsStorageTest extends HBaseMetadataTestCase {

    private IStorageEngine storageEngine;
    private IStorageEngine storageEngine2;
    private CubeInstance cube;
    private CubeInstance cube2;
    private StorageContext context;

    @BeforeClass
    public static void setupResource() throws Exception {
    }

    @AfterClass
    public static void tearDownResource() {
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();

        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());
        cube = cubeMgr.getCube("dic_test");
        cube2 = cubeMgr.getCube("member");
        cube2.setProjectName("lztest");
        System.out.println("projectName:"+cube2.getProjectName());
        Assert.assertNotNull(cube);
        storageEngine = StorageEngineFactory.getStorageEngine(cube);
        storageEngine2 = new ElasticSearchStorageEngine(cube2);
        String url = KylinConfig.getInstanceFromEnv().getStorageUrl();
        context = new StorageContext();
        context.setConnUrl(url);
        context.setEsClusterUrl("10.77.138.46:11930@sparkle");
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test(expected = ScanOutOfLimitException.class)
    @Ignore
    public void testScanOutOfLimit() {
        context.setThreshold(1);
        List<TblColRef> groups = buildGroups();
        List<FunctionDesc> aggregations = buildAggregations();

        search(groups, aggregations, null, context);
    }

    @Test
    public void test01() {
        List<TblColRef> groups = buildGroups();
        List<FunctionDesc> aggregations = buildAggregations();
        TupleFilter filter = buildFilter1(groups.get(0));

        int count = search(groups, aggregations, filter, context);
        assertTrue(count > 0);
    }


    @Test
    public void test02() {
        List<TblColRef> groups = buildGroups2();
        List<FunctionDesc> aggregations = buildAggregations();
        TupleFilter filter = buildAndFilter(groups);

        int count = search2(groups, aggregations, filter, context);
        System.out.println("total:"+count);
        //assertTrue(count > 0);
    }

    @Test
    public void test03() {
        List<TblColRef> groups = buildGroups();
        List<FunctionDesc> aggregations = buildAggregations();
        TupleFilter filter = buildAndFilter(groups);

        int count = search(groups, aggregations, filter, context);
        assertTrue(count > 0);
    }

    @Test
    public void test04() {
        List<TblColRef> groups = buildGroups();
        List<FunctionDesc> aggregations = buildAggregations();
        TupleFilter filter = buildOrFilter(groups);



        int count = search(groups, aggregations, filter, context);
        assertTrue(count > 0);
    }

    @Test
    public void test05() {
        List<TblColRef> groups = buildGroups();
        List<FunctionDesc> aggregations = buildAggregations();

        int count = search(groups, aggregations, null, context);
        assertTrue(count > 0);
    }


    private int search2(List<TblColRef> groups, List<FunctionDesc> aggregations, TupleFilter filter, StorageContext context) {
        int count = 0;
        ITupleIterator iterator = null;
        try {
            List<? extends TupleFilter> f= filter.getChildren();
            for(TupleFilter tf:f){
                System.out.println("*******************:"+tf.getOperator().getValue()) ;
            }
            SQLDigest sqlDigest = new SQLDigest("app.a06_ffan_bi_mem_reg_monthly", filter, null, groups, groups, Collections.<TblColRef> emptySet(), Collections.<TblColRef> emptySet(), aggregations);
            iterator = storageEngine2.search(context, sqlDigest);
            while (iterator.hasNext()) {
                ITuple tuple = iterator.next();
                System.out.println("Tuple = " + tuple);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        return count;
    }

    private int search(List<TblColRef> groups, List<FunctionDesc> aggregations, TupleFilter filter, StorageContext context) {
        int count = 0;
        ITupleIterator iterator = null;
        try {
            List<? extends TupleFilter> f= filter.getChildren();
            for(TupleFilter tf:f){
                System.out.println("*******************:"+tf.getOperator().getValue()) ;
            }
            SQLDigest sqlDigest = new SQLDigest("tmp.test_kylin_dic", filter, null, Collections.<TblColRef> emptySet(), groups, Collections.<TblColRef> emptySet(), Collections.<TblColRef> emptySet(), aggregations);
            iterator = storageEngine.search(context, sqlDigest);
            while (iterator.hasNext()) {
                ITuple tuple = iterator.next();
                System.out.println("Tuple = " + tuple);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        return count;
    }

    private List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TableDesc t1 = new TableDesc();
        t1.setName("TEST_KYLIN_DIC");
        t1.setDatabase("TMP");
        ColumnDesc c1 = new ColumnDesc();
        c1.setName("PLAZA_ID");
        c1.setTable(t1);
        c1.setDatatype("string");
        TblColRef cf1 = new TblColRef(c1);
        groups.add(cf1);

        TableDesc t2 = new TableDesc();
        t2.setName("TEST_KYLIN_DIC");
        t2.setDatabase("TMP");
        ColumnDesc c2 = new ColumnDesc();
        c2.setName("SEX");
        c2.setTable(t2);
        c2.setDatatype("string");
        TblColRef cf2 = new TblColRef(c2);
        groups.add(cf2);

        return groups;
    }

    private List<TblColRef> buildGroups2() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TableDesc t1 = new TableDesc();
        t1.setName("a06_ffan_bi_mem_reg_monthly");
        t1.setDatabase("app");
        ColumnDesc c1 = new ColumnDesc();
        c1.setName("plaza_id");
        c1.setTable(t1);
        c1.setDatatype("string");
        TblColRef cf1 = new TblColRef(c1);
        groups.add(cf1);

        TableDesc t2 = new TableDesc();
        t2.setName("a06_ffan_bi_mem_reg_monthly");
        t2.setDatabase("app");
        ColumnDesc c2 = new ColumnDesc();
        c2.setName("channel_id");
        c2.setTable(t2);
        c2.setDatatype("string");
        TblColRef cf2 = new TblColRef(c2);
        groups.add(cf2);

        TableDesc t3 = new TableDesc();
        t3.setName("a06_ffan_bi_mem_reg_monthly");
        t3.setDatabase("app");
        ColumnDesc c3 = new ColumnDesc();
        c3.setName("datekey");
        c3.setTable(t3);
        c3.setDatatype("string");
        TblColRef cf3 = new TblColRef(c3);
        groups.add(cf3);

        return groups;
    }
    private List<FunctionDesc> buildAggregations() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("MONEY");
        f1.setParameter(p1);
        functions.add(f1);

/*        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("COUNT_DISTINCT");
        ParameterDesc p2 = new ParameterDesc();
        p2.setType("column");
        p2.setValue("SELLER_ID");
        f2.setParameter(p2);
        functions.add(f2);*/

        return functions;
    }

    private CompareTupleFilter buildFilter1(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("1100083");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    private CompareTupleFilter buildFilter2(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("1");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }

    private CompareTupleFilter buildFilter3(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GTE);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("1004");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }

    private CompareTupleFilter buildFilter4(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("310100");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }

    private CompareTupleFilter buildFilter5(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("1");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }
    private CompareTupleFilter buildFilter6(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("2015-01-01");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }

    @SuppressWarnings("unused")
    private TupleFilter buildAndFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter andFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        andFilter.addChild(compareFilter1);
        andFilter.addChild(compareFilter2);
        return andFilter;
    }

    @SuppressWarnings("unused")
    private TupleFilter buildOrFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        CompareTupleFilter compareFilter3 = buildFilter3(columns.get(0));

        LogicalTupleFilter logicFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        LogicalTupleFilter logicFilter2 = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        logicFilter2.addChild(compareFilter3);
        logicFilter2.addChild(compareFilter2);
        logicFilter.addChild(compareFilter1);
        logicFilter.addChild(logicFilter2);
        return logicFilter;
    }

    private TupleFilter buildOrFilter2(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter4(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter5(columns.get(1));
        CompareTupleFilter compareFilter3 = buildFilter6(columns.get(2));

        LogicalTupleFilter logicFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        LogicalTupleFilter logicFilter2 = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        LogicalTupleFilter logicFilter3 = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        logicFilter2.addChild(compareFilter3);
        logicFilter2.addChild(compareFilter2);
        logicFilter3.addChild(compareFilter3);
        logicFilter3.addChild(compareFilter1);
        logicFilter.addChild(logicFilter3);
        logicFilter.addChild(logicFilter2);
        return logicFilter;
    }
    String cmd="--------------:";
    private class InnerClass{

        String name="-------";
        int op=0;
        int partentOP=0;
    }
    @Test
    public void test06() {
        List<TblColRef> groups = buildGroups();
        List<FunctionDesc> aggregations = buildAggregations();
        TupleFilter filter = buildOrFilter(groups);
        TupleFilter tt=filter.flatFilter();
        QueryBuilder qb=null;
        int j=0;
        InnerClass ic=new InnerClass();
        BoolQueryBuilder bqb=new BoolQueryBuilder();
        List<QueryBuilder> listc=new ArrayList<>();
        scanFilter2(filter, ic);
        System.out.println(ic.name);


    }

    @Test
    public void test07() {
        List<TblColRef> groups = buildGroups2();
        TupleFilter filter = buildOrFilter2(groups);
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).put("cluster.name", "sparkle").build();

        Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("10.77.138.46", 11930));
        InnerClass ic=new InnerClass();
        BoolQueryBuilder bqb=new BoolQueryBuilder();

        List<QueryBuilder> listc=new ArrayList<>();
        List<BoolQueryBuilder> listb=new ArrayList<BoolQueryBuilder>();
        setLevel(filter,1);

        scanFilter2(filter, ic);
        System.out.println("@@@@@@@@@"+stack.size());

        System.out.println("*************:"+stack.peek().queryBuilder);
        SearchResponse response = client.prepareSearch("ffanbi")
                .setTypes("a06_ffan_bi_channel_analysis")
                .addFields(new String[]{"city_name","datekey","channel_id","plaza_name"})
                .setQuery(stack.pop().queryBuilder)
                .execute().actionGet();

        System.out.println(response);
/*        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println("-----------");
            for (Map.Entry<String, Object> e : searchHit.getSource().entrySet()) {
                System.out.println(e.getKey() + ":" + e.getValue());
            }
        }*/

        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println("-----------");
            for (Map.Entry<String, SearchHitField> e : searchHit.getFields().entrySet()) {

                System.out.println(e.getKey() + ":" + e.getValue().getValues().get(0));
            }
        }




    }
    int i=1;

    @Test
    public void test08() {
        List<TblColRef> groups = buildGroups2();
        TupleFilter filter = buildOrFilter2(groups);
        TupleFilter f=filter.flatFilter();
        setLevel(filter,1);
        scanFilter3(filter);
    }


    private void  setLevel(TupleFilter filter, int j) {

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
    private void  scanFilter3(TupleFilter filter ) {

        if (filter.hasChildren()) {
            List<? extends TupleFilter>  litf=filter.getChildren();
            for (TupleFilter f : litf) {
                if (f.getOperator().getValue() != 30 && f.getOperator().getValue() != 31) {
                    scanFilter3(f);

                }

            }


        }
        System.out.println(":op:" + filter.getOperator().getValue()+":value:" + filter.getValues()+"level:"+filter.level);


    }

    Stack<QueryBuilderUtil> stack = new Stack<QueryBuilderUtil>();
    private void  scanFilter2(TupleFilter filter,InnerClass ic ) {



        if (filter.hasChildren()) {
            List<? extends TupleFilter>  litf=filter.getChildren();
            for (TupleFilter f : litf) {
                if (f.getOperator().getValue() != 30 && f.getOperator().getValue() != 31) {
                    scanFilter2(f,ic);

                }

            }

        }

        if(filter instanceof LogicalTupleFilter){

            LogicalTupleFilter cf=(LogicalTupleFilter)filter;
            ic.name=ic.name+" new BoolQueryBuilder()";

            int tmpop=cf.getOperator().getValue();
            BoolQueryBuilder bb=new BoolQueryBuilder();

            int level=filter.level+1;
            System.out.println("level:"+stack.empty());
            while(!stack.empty()) {

                if (stack.peek().level == level) {
                    QueryBuilder qb = stack.pop().queryBuilder;
                    if (tmpop == 20) {
                        bb.must(qb);
                    }
                    if (tmpop == 21) {
                        bb.should(qb);
                    }
                }else{break;}
            }

            QueryBuilderUtil qbu=new QueryBuilderUtil(bb,filter.level);

            stack.push(qbu);

            System.out.println("push bool:"+i++);

        }



        if(filter instanceof CompareTupleFilter){
            CompareTupleFilter cf=(CompareTupleFilter)filter;
            //cmd=cmd+" "+cf.getColumn().getName()+"="+cf.getFirstValue();

            TermQueryBuilder tqb=null;
            if(ic.partentOP==20){
                tqb=termQuery(cf.getColumn().getName(), cf.getFirstValue());
            }
            if (ic.partentOP==21){
                tqb=termQuery(cf.getColumn().getName(),cf.getFirstValue());
            }
            if (ic.partentOP==0){
                tqb=termQuery(cf.getColumn().getName(),cf.getFirstValue());
            }

            QueryBuilderUtil qbdu=new QueryBuilderUtil(tqb,filter.level);
            stack.push(qbdu);

            System.out.println("push term:"+"level:"+qbdu.level+"   "+i++);

        }


    }
    private void  scanFilter(TupleFilter filter ) {
        /*System.out.println(i+":op:"+filter.getOperator().getValue());
        System.out.println(i+":value:"+filter.getValues());
        System.out.println("---------------------------------");


        //bqb = QueryBuilders.boolQuery();

        if(filter instanceof ColumnTupleFilter){

            System.out.println("ColumnTupleFilter");
        }else if(filter instanceof CompareTupleFilter){
            System.out.println("CompareTupleFilter");
            CompareTupleFilter cf=(CompareTupleFilter)filter;
            qb=termQuery(cf.getColumn().getName(),cf.getValues().toArray()[0]);


        }else if(filter instanceof LogicalTupleFilter){

            System.out.println("LogicalTupleFilter");
            bqb = boolQuery();


        }else if(filter instanceof ConstantTupleFilter){

            System.out.println("ConstantTupleFilter");

        }else {
            System.out.println("other");
        }*/
        if (filter.hasChildren()) {
            i++;
            for (TupleFilter f : filter.getChildren()) {
/*                QueryBuilder=
                if (filter.getOperator().getValue()==20){


                    bqd.must(termQuery(f.g, "test1"));
                }else if (filter.getOperator().getValue()==21){

                }else {

                }*/
                if (f.getOperator().getValue() != 30 && f.getOperator().getValue() != 31) {


                    scanFilter(f);
/*                    System.out.println(i + ":op:" + f.getOperator().getValue());
                    System.out.println(i + ":value:" + f.getValues().toArray());
                    System.out.println("---------------------------------");*/
                }
            }
    /*        System.out.println(i + ":op:" + filter.getOperator().getValue());
            System.out.println(i + ":value:" + filter.getValues());
            System.out.println("---------------------------------");*/

        } else {

            //  System.out.println( "**" + filter.getOperator().getValue() + "**" + filter.getValues());

  /*          if(filter instanceof ColumnTupleFilter) {
                CompareTupleFilter fff = (CompareTupleFilter) filter;
                System.out.println(fff.getColumn() + "**" + fff.getOperator().getValue() + "**" + fff.getValues());
            }*/
        }
    }
}

