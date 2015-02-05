package org.apache.kylin.bootstrap;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ErrorReportValve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Overrides the error message (while still passing through the status code).
 */
public class MinimalErrorReportValve extends ErrorReportValve {
	private static final Logger log = LoggerFactory.getLogger(MinimalErrorReportValve.class);

	@Override
	protected void report(Request request, Response response, Throwable ex) {
		if (!response.isError()) {
			return;
		}

		String message = response.getMessage();
		if (message == null || message.isEmpty()) {
			message = "ERROR";
		}

		int status = response.getStatus();
		String url = request.getRequestURI();
		int slash = url.indexOf('/', 9);
		if (slash > 0) {
			url = url.substring(slash);
		}

		String info = url+": ("+status+") "+message;

		if (status == 404) {
			log.warn(info, ex);

		} else {
			log.error(info, ex);
		}

		try {
			PrintWriter writer = response.getReporter();
			writer.print(message);
			if (ex != null) {
				ex.printStackTrace(writer);
			}
			response.finishResponse();

		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}
}
