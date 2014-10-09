package com.kylinolap.rest.metrics;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlet.InstrumentedFilterContextListener;

public class KylinInstrumentedFilterContextListener implements ServletContextListener {

    @Autowired
    private MetricRegistry metricRegistry;

    private final InnerKylinInstrumentedFilterContextListener innerKylinInstrumentedFilterContextListener =
            new InnerKylinInstrumentedFilterContextListener();

    @Override
    public void contextInitialized(ServletContextEvent event) {
        WebApplicationContextUtils.getRequiredWebApplicationContext(event.getServletContext())
                .getAutowireCapableBeanFactory().autowireBean(this);

        innerKylinInstrumentedFilterContextListener.contextInitialized(event);
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
    }

    class InnerKylinInstrumentedFilterContextListener extends InstrumentedFilterContextListener {

        @Override
        protected MetricRegistry getMetricRegistry() {
            return metricRegistry;
        }

    }

}
