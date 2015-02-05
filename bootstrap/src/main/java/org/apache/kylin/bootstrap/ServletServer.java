package org.apache.kylin.bootstrap;

import java.util.Map;

public interface ServletServer {

	String getName();

	void start(Map<String, String> contexts, int httpPort, int httpsPort, String keystoreFile, String keystorePass)
			throws Exception;

	void stop() throws Exception;
}
