package org.apache.kylin.common.persistence;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liuze on 2016/1/8 0008.
 */
public class ElasticSearchClient {



    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

    private static final Map<String, Client> ConnPoolES = new ConcurrentHashMap<String, Client>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (Client client : ConnPoolES.values()) {
                    client.close();

                }
            }
        });
    }

    public static Client get(String esClusterUrl) {

        Client client = ConnPoolES.get(esClusterUrl);
        try {
            if (client == null) {
                Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).put("cluster.name", getClusterName(esClusterUrl)).build();
                client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(getClusterHost(esClusterUrl), getClusterPort(esClusterUrl)));
                ConnPoolES.put(esClusterUrl, client);
            }
        } catch (Throwable t) {
            throw new StorageException("Error when open client " + esClusterUrl, t);
        }

        return client;
    }


    private static String getClusterName(String esClusterUrl){
        return esClusterUrl.split("@")[1];

    }

    private static String getClusterHost(String esClusterUrl){
        return esClusterUrl.split("@")[0].split(":")[0];

    }

    private static int getClusterPort(String esClusterUrl){
        return Integer.parseInt(esClusterUrl.split("@")[0].split(":")[1]);

    }


}
