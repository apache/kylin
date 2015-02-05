package org.apache.kylin.bootstrap;

import java.util.Map;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.webapp.WebAppContext;

class JettyServletServer implements ServletServer {
	private Server server;

	public String getName() {
		return "Jetty";
	}

	public void start(Map<String, String> contexts, int httpPort, int httpsPort, String keystoreFile, String keystorePass)
		throws Exception {

		if (server != null) {
			throw new IllegalStateException("Web server is already running.");
		}

		server = new Server(httpPort);

		for (String contextPath : contexts.keySet()) {
			WebAppContext webapp = new WebAppContext();
			webapp.setContextPath(contextPath);
			webapp.setWar(contexts.get(contextPath));

			// wire up DefaultServlet for static files
			webapp.addServlet(DefaultServlet.class, "/*");

			server.setHandler(webapp);
		}
		server.start();
	}

	public void stop() throws Exception {
		if (server == null) {
			throw new IllegalStateException("Web server is not running.");
		}

		server.stop();

		server = null;
	}
}
