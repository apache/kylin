package org.apache.kylin.rest.broadcaster;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Created by qianzhou on 1/16/15.
 */
public class BroadcasterReceiveServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public static interface BroadcasterHandler {

        void handle(String type, String name, String event);
    }

    private BroadcasterHandler handler;

    public BroadcasterReceiveServlet(BroadcasterHandler handler) {
        this.handler = handler;
    }

    private static final Pattern PATTERN = Pattern.compile("/(.+)/(.+)/(.+)");
    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        handle(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        handle(req, resp);
    }

    private void handle(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final String startString = "/kylin/api/cache";
        final String requestURI = req.getRequestURI();
        final String substring = requestURI.substring(requestURI.indexOf(startString) + startString.length());
        final Matcher matcher = PATTERN.matcher(substring);
        if (matcher.matches()) {
            String type = matcher.group(1);
            String name = matcher.group(2);
            String event = matcher.group(3);
            if (handler != null) {
                handler.handle(type, name, event);
            }
            resp.getWriter().write("type:" + type + " name:" + name + " event:" + event);
        } else {
            resp.getWriter().write("not valid uri");
        }
        resp.getWriter().close();
    }
}
