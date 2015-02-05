package org.apache.kylin.bootstrap;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap {

	private static final Logger log = LoggerFactory.getLogger(Bootstrap.class);

	private static final long MS_PER_NANO = (long)1e6;
	private static final String SEPARATOR = "========================================================================";
	private static final String HELP = "java -jar bootstrap.jar\n"+
			"  --help                 : this help text\n"+
			"  -war <context>=<path>  : set the context of war (e.g., /=./root.war)\n" +
			"  -p <port>              : set the HTTP listening port (default: 8080)\n"+
			"  -s <port>              : set the HTTPS listening port (default: none)\n" +
			"  -ks <keystore=file>    : set the path to the keystore file (default: null)\n" +
			"  -kp <keystore-pass>    : set the password to the keystore (default: null)\n" +
			"  --tomcat               : use Tomcat as servlet container (default)\n" +
			"  --jetty                : use Jetty as servlet container\n" +
			"  --glassfish            : use GlassFish as servlet container";

	public static void main(String[] args) {

		ServletServer server = null;
		try {
			server = serverStart(args);

		} catch (Exception ex) {
			ex.printStackTrace();
			log.error(ex.getMessage(), ex);
		}

		long start = System.nanoTime();

		try {
			if (server != null) {
				waitForKillSignal();
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			log.error(ex.getMessage(), ex);
		}

		log.info("Killing webhead, uptime: %d ms", (System.nanoTime() - start) / MS_PER_NANO);

		try {
			if (server != null) {
				serverStop(server);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			log.error(ex.getMessage(), ex);
		}
	}

	private static void waitForKillSignal()
			throws IOException {

			log.info("Press ENTER to exit.");
			log.info(SEPARATOR);

			System.in.read();
	}

	private static ServletServer serverStart(String[] args)
			throws IOException, Exception {

		log.info(SEPARATOR);
		log.info("WAR Bootstrap");
		log.info(SEPARATOR);

		Map<String, String> contexts = new HashMap<String, String>();
		int port = 8080;
		int https = -1;
		ServletServer server = null;
		String keystoreFile = null;
		String keystorePass = null;

		for (int i=0; i<args.length; i++) {
			String arg = args[i];
			if ("-p".equals(arg)) {
				port = Integer.parseInt(args[++i]);

			} else if ("-s".equals(arg)) {
				https = Integer.parseInt(args[++i]);

			} else if ("-ks".equals(arg)) {
				keystoreFile = args[++i];

			} else if ("-kp".equals(arg)) {
				keystorePass = args[++i];

			} else if ("-war".equals(arg)) {
				String contextPath;
				String warPath = args[++i];
				int delim = warPath.indexOf('=');
				if (delim < 1) {
					contextPath = "/";
				} else {
					contextPath = warPath.substring(0, delim);
				}
				warPath = new File(warPath.substring(delim+1)).getCanonicalPath();
				contexts.put(contextPath, warPath);

			} else if ("--jetty".equalsIgnoreCase(arg)) {
				server = new JettyServletServer();

			} else if ("--tomcat".equalsIgnoreCase(arg)) {
				server = new TomcatServletServer();

			} else if ("--help".equalsIgnoreCase(arg)) {
				log.info(HELP);
				log.info(SEPARATOR);
				return null;

			} else {
				log.info(HELP);
				log.info(SEPARATOR);
				return null;
			}
		}

		if (server == null) {
			server = new TomcatServletServer();
		}

		log.info(" - servlet container: "+server.getName());

		for (String contextPath : contexts.keySet()) {
			log.info(" - context root: "+contextPath);
			log.info(" - war path: "+contexts.get(contextPath));
		}
		if (port > 0) {
			log.info(" - Listening to HTTP on port: "+port);
		} else {
			log.info(" - Not listening to HTTP");
		}
		if (https > 0) {
			log.info(" - Listening to HTTPS on port: "+https);
		} else {
			log.info(" - Not listening to HTTPS");
		}

		log.info(SEPARATOR);
		server.start(contexts, port, https, keystoreFile, keystorePass);
		log.info(SEPARATOR);

		return server;
	}

	private static void serverStop(ServletServer server)
			throws Exception {

		log.info(SEPARATOR);
		log.info("WAR Bootstrap exiting...");
		log.info(SEPARATOR);

		server.stop();
	}
}
