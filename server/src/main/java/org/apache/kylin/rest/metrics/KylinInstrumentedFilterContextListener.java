/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.rest.metrics;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlet.InstrumentedFilterContextListener;

public class KylinInstrumentedFilterContextListener implements ServletContextListener {

    @Autowired
    private MetricRegistry metricRegistry;

    private final InnerKylinInstrumentedFilterContextListener innerKylinInstrumentedFilterContextListener = new InnerKylinInstrumentedFilterContextListener();

    @Override
    public void contextInitialized(ServletContextEvent event) {
        WebApplicationContextUtils.getRequiredWebApplicationContext(event.getServletContext()).getAutowireCapableBeanFactory().autowireBean(this);

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
