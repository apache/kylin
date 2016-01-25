package org.apache.kylin.job.hadoop.cube;


import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.common.persistence.ElasticSearchClient;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.TblColRef;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liuze on 2016/1/13 0013.
 */
public class BulkESMapper<KEYIN> extends KylinMapper<KEYIN, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(BulkESMapper.class);



    private String cubeName;
    private String segmentName;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;


    private int counter;


    private String[] fields;

    private String index;

    private String type;

    private Client client;

    private BulkProcessor bulkProcessor;

    @Override
    protected void setup(Context context) throws IOException {

        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        List<TblColRef> columns =cube.getAllColumns();
        this.fields=new String[columns.size()];
        int i=0;


        String esUrl=config.getKylinEsClusterUrl();
        //this.index=cube.getProjectName();
        this.type="kylin";
        this.index=cubeName.toLowerCase();
        this.client = ElasticSearchClient.get(esUrl);
        IndicesExistsRequest request = new IndicesExistsRequestBuilder(client.admin().indices(), index).request();
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if(response.isExists()) {
            client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();

        }
        client.admin().indices().create(new CreateIndexRequest(index)).actionGet();

        XContentBuilder mapping = jsonBuilder()
                .startObject()
                .startObject("properties");

        for(TblColRef tcr:columns){
            fields[i]=tcr.getName();
            i++;
            if(tcr.getType().isIntegerFamily()){
                if(tcr.getType().isBigInt()) {
                    mapping.startObject(tcr.getName()).field("type", "long").field("index", "not_analyzed").endObject();
                    continue;
                }else{
                    mapping.startObject(tcr.getName()).field("type", "integer").field("index", "not_analyzed").endObject();
                    continue;
                }
            }
            if(tcr.getType().isDateTimeFamily()){
                mapping.startObject(tcr.getName()).field("type","string").field("index", "not_analyzed").endObject();
                continue;
            }
            if(tcr.getType().isStringFamily()){
                mapping.startObject(tcr.getName()).field("type","string").field("index", "not_analyzed").endObject();
                continue;
            }
            if(tcr.getType().isNumberFamily()){
                mapping.startObject(tcr.getName()).field("type","double").field("index", "not_analyzed").endObject();
                continue;
            }

        }

        mapping.endObject().endObject();
        System.out.println("mapping:"+mapping.string());
        PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type).source(mapping.string());
        client.admin().indices().putMapping(mappingRequest).actionGet();

        bulkProcessor = BulkProcessor.builder(client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {

                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request,  BulkResponse response) {

                    }
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        System.out.println("happen fail = " + failure.getMessage() + " cause = " +failure.getCause());
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(20, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .build();

        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);

    }



    @Override
    public void map(KEYIN key, Text value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
        String[] line=value.toString().split("\177");

        Map<String, Object> json = new HashMap<String, Object>();

        if(line.length==this.fields.length){
            for (int i=0;i<line.length;i++){

                json.put(this.fields[i],line[i]);

            }

            bulkProcessor.add(new IndexRequest(index, type,null).source(json));
        }



    }


    @Override
    protected void cleanup(Context context) throws IOException {

        if(this.bulkProcessor!=null){
            bulkProcessor.close();
        }
        if(this.client!=null){
            client.close();
        }

    }
}
