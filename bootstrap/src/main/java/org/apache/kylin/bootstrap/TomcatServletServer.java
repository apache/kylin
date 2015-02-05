package org.apache.kylin.bootstrap;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

import org.apache.catalina.Host;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.startup.ContextConfig;
import org.apache.catalina.startup.Tomcat;

class TomcatServletServer implements ServletServer {
	private Tomcat server;
	private File tmpDir;

	public String getName() {
		return "Tomcat";
	}

	public void start(Map<String, String> contexts, int httpPort, int httpsPort, String keystoreFile, String keystorePass)
			throws Exception {

		if (server != null) {
			throw new IllegalStateException("Web server is already running.");
		}

		tmpDir = new File("tomcat");

		server = new Tomcat();
		if (httpsPort > 0) {
			Connector httpsConnector = new Connector();
			httpsConnector.setPort(httpsPort);
			httpsConnector.setSecure(true);
			httpsConnector.setScheme("https");
			httpsConnector.setAttribute("keystoreFile", keystoreFile);
			httpsConnector.setAttribute("keystorePass", keystorePass);
			httpsConnector.setAttribute("clientAuth", "false");
			httpsConnector.setAttribute("sslProtocol", "TLS");
			httpsConnector.setAttribute("SSLEnabled", true);
			server.getService().addConnector(httpsConnector);
		}

		server.setPort(httpPort);
		server.setBaseDir(tmpDir.getCanonicalPath());

		boolean loadJSP = getClass().getResource("/javax/servlet/jsp/resources/jsp_2_0.xsd") != null;

		Host host = server.getHost();
		if (host instanceof StandardHost) {
			((StandardHost)host).setErrorReportValveClass(MinimalErrorReportValve.class.getCanonicalName());
		}

		LifecycleListener minListener = loadJSP ? null : new MinimalLifecycleListener();

		for (String contextPath : contexts.keySet()) {
			String warPath = contexts.get(contextPath);
			if (warPath == null || warPath.isEmpty()) {
				continue;
			}

			if (loadJSP) {
				// alternatively do this to include JSP
				server.addWebapp(contextPath, warPath);

			} else {

				final StandardContext cx = new StandardContext();
				cx.setName(contextPath);
				cx.setPath(contextPath);
				cx.setDocBase(warPath);
				cx.setUnpackWAR(true);
				cx.setProcessTlds(false);

				cx.addLifecycleListener(minListener);
			
				ContextConfig config = new MinimalContextConfig(cx);
				cx.addLifecycleListener(config);

				// prevent it from looking ( if it finds one - it'll have dup error )
				// "org/apache/catalin/startup/NO_DEFAULT_XML"
				config.setDefaultWebXml(server.noDefaultWebXmlPath());

				host.addChild(cx);
			}
		}

		server.start();
	}

	public void stop() throws Exception {
		if (server == null) {
			throw new IllegalStateException("Web server is not running.");
		}

		server.stop();
		server.destroy();

		if (tmpDir != null) {
			Deque<File> stack = new ArrayDeque<File>();
			stack.push(tmpDir);
			while (stack.size() > 0) {
				File parent = stack.pop();
				if (parent.isDirectory()) {
					File[] children = parent.listFiles();
					if (children.length > 0) {
						stack.push(parent);
						for (File child : children) {
							stack.push(child);
						}
					}
				}
				parent.delete();
			}
			tmpDir = null;
		}

		server = null;
	}
}
