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

package org.apache.kylin.ext;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.TrackedWebResource;
import org.apache.catalina.WebResource;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.WebResourceSet;

public class OrderedWebResourceRoot implements WebResourceRoot {

    private static final String WEB_INF_LIB_PATH = "/WEB-INF/lib";

    private static final Comparator<WebResource> WEB_RESOURCE_COMPARATOR = new Comparator<WebResource>() {
        @Override
        public int compare(WebResource o1, WebResource o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };

    private WebResourceRoot delegate;

    public OrderedWebResourceRoot(WebResourceRoot delegate) {
        this.delegate = delegate;
    }

    @Override
    public WebResource[] listResources(String path) {
        WebResource[] webResources = delegate.listResources(path);

        if (WEB_INF_LIB_PATH.equals(path)) {
            Arrays.sort(webResources, WEB_RESOURCE_COMPARATOR);
        }

        return webResources;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        delegate.addLifecycleListener(listener);
    }

    @Override
    public LifecycleListener[] findLifecycleListeners() {
        return delegate.findLifecycleListeners();
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        delegate.removeLifecycleListener(listener);
    }

    @Override
    public void init() throws LifecycleException {
        delegate.init();
    }

    @Override
    public void start() throws LifecycleException {
        delegate.start();
    }

    @Override
    public void stop() throws LifecycleException {
        delegate.stop();
    }

    @Override
    public void destroy() throws LifecycleException {
        delegate.destroy();
    }

    @Override
    public LifecycleState getState() {
        return delegate.getState();
    }

    @Override
    public String getStateName() {
        return delegate.getStateName();
    }

    @Override
    public WebResource getResource(String path) {
        return delegate.getResource(path);
    }

    @Override
    public WebResource[] getResources(String path) {
        return delegate.getResources(path);
    }

    @Override
    public WebResource getClassLoaderResource(String path) {
        return delegate.getClassLoaderResource(path);
    }

    @Override
    public WebResource[] getClassLoaderResources(String path) {
        return delegate.getClassLoaderResources(path);
    }

    @Override
    public String[] list(String path) {
        return delegate.list(path);
    }

    @Override
    public Set<String> listWebAppPaths(String path) {
        return delegate.listWebAppPaths(path);
    }

    @Override
    public boolean mkdir(String path) {
        return delegate.mkdir(path);
    }

    @Override
    public boolean write(String path, InputStream is, boolean overwrite) {
        return delegate.write(path, is, overwrite);
    }

    @Override
    public void createWebResourceSet(ResourceSetType type, String webAppMount, URL url, String internalPath) {
        delegate.createWebResourceSet(type, webAppMount, url, internalPath);
    }

    @Override
    public void createWebResourceSet(ResourceSetType type, String webAppMount, String base, String archivePath,
                                     String internalPath) {
        delegate.createWebResourceSet(type, webAppMount, base, archivePath, internalPath);
    }

    @Override
    public void addPreResources(WebResourceSet webResourceSet) {
        delegate.addPreResources(webResourceSet);
    }

    @Override
    public WebResourceSet[] getPreResources() {
        return delegate.getPreResources();
    }

    @Override
    public void addJarResources(WebResourceSet webResourceSet) {
        delegate.addJarResources(webResourceSet);
    }

    @Override
    public WebResourceSet[] getJarResources() {
        return delegate.getJarResources();
    }

    @Override
    public void addPostResources(WebResourceSet webResourceSet) {
        delegate.addPostResources(webResourceSet);
    }

    @Override
    public WebResourceSet[] getPostResources() {
        return delegate.getPostResources();
    }

    @Override
    public Context getContext() {
        return delegate.getContext();
    }

    @Override
    public void setContext(Context context) {
        delegate.setContext(context);
    }

    @Override
    public void setAllowLinking(boolean allowLinking) {
        delegate.setAllowLinking(allowLinking);
    }

    @Override
    public boolean getAllowLinking() {
        return delegate.getAllowLinking();
    }

    @Override
    public void setCachingAllowed(boolean cachingAllowed) {
        delegate.setCachingAllowed(cachingAllowed);
    }

    @Override
    public boolean isCachingAllowed() {
        return delegate.isCachingAllowed();
    }

    @Override
    public void setCacheTtl(long ttl) {
        delegate.setCacheTtl(ttl);
    }

    @Override
    public long getCacheTtl() {
        return delegate.getCacheTtl();
    }

    @Override
    public void setCacheMaxSize(long cacheMaxSize) {
        delegate.setCacheMaxSize(cacheMaxSize);
    }

    @Override
    public long getCacheMaxSize() {
        return delegate.getCacheMaxSize();
    }

    @Override
    public void setCacheObjectMaxSize(int cacheObjectMaxSize) {
        delegate.setCacheObjectMaxSize(cacheObjectMaxSize);
    }

    @Override
    public int getCacheObjectMaxSize() {
        return delegate.getCacheObjectMaxSize();
    }

    @Override
    public void setTrackLockedFiles(boolean trackLockedFiles) {
        delegate.setTrackLockedFiles(trackLockedFiles);
    }

    @Override
    public boolean getTrackLockedFiles() {
        return delegate.getTrackLockedFiles();
    }

    @Override
    public void backgroundProcess() {
        delegate.backgroundProcess();
    }

    @Override
    public void registerTrackedResource(TrackedWebResource trackedResource) {
        delegate.registerTrackedResource(trackedResource);
    }

    @Override
    public void deregisterTrackedResource(TrackedWebResource trackedResource) {
        delegate.deregisterTrackedResource(trackedResource);
    }

    @Override
    public List<URL> getBaseUrls() {
        return delegate.getBaseUrls();
    }

    @Override
    public void gc() {
        delegate.gc();
    }

}
