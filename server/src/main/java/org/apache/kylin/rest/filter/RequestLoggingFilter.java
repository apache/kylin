package org.apache.kylin.rest.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.AbstractRequestLoggingFilter;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by jiazhong on 2015/4/14.
 */
public class RequestLoggingFilter extends AbstractRequestLoggingFilter {

    private static final Logger logger = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Override
    protected void beforeRequest(HttpServletRequest request, String message) {
//        String user = SecurityContextHolder.getContext().getAuthentication().getName();
//        logger.info("USER:"+user+"requestURI:"+request.getRequestURI());
    }

    @Override
    protected void afterRequest(HttpServletRequest request, String message)
    {
        logger.info(message);
//        logger.info("["+"uri="+request.getRequestURI()+",user="+request.getRemoteUser()+request.get);
    }

}