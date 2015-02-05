package org.apache.kylin.bootstrap;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.catalina.Context;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleEvent;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Wrapper;

class MinimalLifecycleListener implements LifecycleListener{
	private static final String MIME_TYPES_RESOURCE = "/MIMETypes.properties";
	private final Properties MIME_TYPES;

	public MinimalLifecycleListener() {
		MIME_TYPES = new Properties();
		try {
			InputStream stream = MinimalLifecycleListener.class.getResourceAsStream(MIME_TYPES_RESOURCE);
			MIME_TYPES.load(stream);

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public void lifecycleEvent(LifecycleEvent event) {
		if (!Lifecycle.BEFORE_START_EVENT.equals(event.getType())) {
			return;
		}

		Context cx = (Context)event.getLifecycle();

		// default servlet
		Wrapper servlet = cx.createWrapper();
		servlet.setName("default");
		servlet.setServletClass("org.apache.catalina.servlets.DefaultServlet");
		servlet.setLoadOnStartup(1);
		servlet.setOverridable(true);
		cx.addChild(servlet);

		// servlet mappings
		cx.addServletMapping("/", "default");

		// MIME mappings
		for (String extension : MIME_TYPES.stringPropertyNames()) {
			String mimeType = MIME_TYPES.getProperty(extension);
			cx.addMimeMapping(extension, mimeType);
		}

		// welcome files
		cx.addWelcomeFile("index.html");
	}
}
