package com.kylinolap.rest.broadcaster;

import com.kylinolap.common.restclient.Broadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by qianzhou on 1/16/15.
 */
public class BroadcasterReceiveServlet extends HttpServlet {

    public static interface BroadcasterHandler {

        void handle(String type, String name, String event);
    }

    private BroadcasterHandler handler;

    public BroadcasterReceiveServlet(BroadcasterHandler handler) {
        this.handler = handler;
    }

    private static Logger logger = LoggerFactory.getLogger(BroadcasterReceiveServlet.class);

    private static final Pattern PATTERN = Pattern.compile("/(\\w+)/(\\w+)/(\\w+)");
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
