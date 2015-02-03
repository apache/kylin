package org.apache.kylin.job.tools;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by qianzhou on 1/20/15.
 */
public class HadoopStatusGetter {

    private final String mrJobId;
    private final String yarnUrl;

    protected static final Logger log = LoggerFactory.getLogger(HadoopStatusChecker.class);

    public HadoopStatusGetter(String yarnUrl, String mrJobId) {
        this.yarnUrl = yarnUrl;
        this.mrJobId = mrJobId;
    }

    public Pair<RMAppState, FinalApplicationStatus> get() throws IOException {
        String applicationId = mrJobId.replace("job", "application");
        String url = yarnUrl.replace("${job_id}", applicationId);
        JsonNode root = new ObjectMapper().readTree(getHttpResponse(url));
        RMAppState state = RMAppState.valueOf(root.findValue("state").getTextValue());
        FinalApplicationStatus finalStatus = FinalApplicationStatus.valueOf(root.findValue("finalStatus").getTextValue());
        return Pair.of(state, finalStatus);
    }

    private String getHttpResponse(String url) throws IOException {
        HttpClient client = new HttpClient();

        String response = null;
        while (response == null) { // follow redirects via 'refresh'
            if (url.startsWith("https://")) {
                registerEasyHttps();
            }
            if (url.contains("anonymous=true") == false) {
                url += url.contains("?") ? "&" : "?";
                url += "anonymous=true";
            }

            HttpMethod get = new GetMethod(url);
            try {
                client.executeMethod(get);

                String redirect = null;
                Header h = get.getResponseHeader("Refresh");
                if (h != null) {
                    String s = h.getValue();
                    int cut = s.indexOf("url=");
                    if (cut >= 0) {
                        redirect = s.substring(cut + 4);
                    }
                }

                if (redirect == null) {
                    response = get.getResponseBodyAsString();
                    log.debug("Job " + mrJobId + " get status check result.\n");
                } else {
                    url = redirect;
                    log.debug("Job " + mrJobId + " check redirect url " + url + ".\n");
                }
            } finally {
                get.releaseConnection();
            }
        }

        return response;
    }



    private static Protocol EASY_HTTPS = null;

    private static void registerEasyHttps() {
        // by pass all https issue
        if (EASY_HTTPS == null) {
            EASY_HTTPS = new Protocol("https", (ProtocolSocketFactory) new DefaultSslProtocolSocketFactory(), 443);
            Protocol.registerProtocol("https", EASY_HTTPS);
        }
    }

}
