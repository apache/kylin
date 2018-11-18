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

package org.apache.kylin.storage.druid.read.cursor;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.druid.http.ClientResponse;
import org.apache.kylin.storage.druid.http.HttpClient;
import org.apache.kylin.storage.druid.http.HttpResponseHandler;
import org.apache.kylin.storage.druid.http.Request;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class HttpDruidClient<T> implements DruidClient<T> {
    private static final Logger logger = LoggerFactory.getLogger(HttpDruidClient.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final TypeReference<T> typeReference;
    private final String host;
    private final boolean isSmile;

    public HttpDruidClient(HttpClient httpClient, ObjectMapper objectMapper, TypeReference<T> typeReference, String host) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.typeReference = typeReference;
        this.host = host;
        this.isSmile = this.objectMapper.getFactory() instanceof SmileFactory;
    }

    @SuppressWarnings("checkstyle:methodlength")
    @Override
    public Cursor<T> execute(final Query<T> query, final StorageContext context) {
        final ListenableFuture<InputStream> future;
        final String url = String.format(Locale.ROOT, "http://%s/druid/v2/", host);
        final long requestStartTime = System.currentTimeMillis();

        final HttpResponseHandler<InputStream, InputStream> responseHandler = new HttpResponseHandler<InputStream, InputStream>() {
            private final AtomicLong byteCount = new AtomicLong(0);
            private final BlockingQueue<InputStream> queue = new LinkedBlockingQueue<>();
            private final AtomicBoolean done = new AtomicBoolean(false);
            private long responseStartTime;

            @Override
            public ClientResponse<InputStream> handleResponse(HttpResponse response) {
                responseStartTime = System.currentTimeMillis();

                try {
                    queue.put(new ChannelBufferInputStream(response.getContent()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
                byteCount.addAndGet(response.getContent().readableBytes());
                return ClientResponse.<InputStream>finished(
                        new SequenceInputStream(
                                new Enumeration<InputStream>() {
                                    @Override
                                    public boolean hasMoreElements() {
                                        // Done is always true until the last stream has be put in the queue.
                                        // Then the stream should be spouting good InputStreams.
                                        synchronized (done) {
                                            return !done.get() || !queue.isEmpty();
                                        }
                                    }

                                    @Override
                                    public InputStream nextElement() {
                                        try {
                                            return queue.take();
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            throw Throwables.propagate(e);
                                        }
                                    }
                                }
                        )
                );
            }

            @Override
            public ClientResponse<InputStream> handleChunk(
                    ClientResponse<InputStream> clientResponse, HttpChunk chunk
            ) {
                final ChannelBuffer channelBuffer = chunk.getContent();
                final int bytes = channelBuffer.readableBytes();
                if (bytes > 0) {
                    try {
                        queue.put(new ChannelBufferInputStream(channelBuffer));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                    byteCount.addAndGet(bytes);
                }
                return clientResponse;
            }

            @Override
            public ClientResponse<InputStream> done(ClientResponse<InputStream> clientResponse) {
                // FIXME collect druid scan bytes instead of http response size
                context.getQueryContext().addAndGetScannedBytes(byteCount.get());

                logger.debug("Completed request for queryId={} intervals={} bytes={} ttfb={} time={}",
                        query.getId(),
                        query.getIntervals(),
                        byteCount.get(),
                        responseStartTime - requestStartTime,
                        System.currentTimeMillis() - requestStartTime);


                synchronized (done) {
                    try {
                        // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
                        // after done is set to true, regardless of the rest of the stream's state.
                        queue.put(ByteSource.empty().openStream());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    } catch (IOException e) {
                        // This should never happen
                        throw Throwables.propagate(e);
                    } finally {
                        done.set(true);
                    }
                }
                return ClientResponse.finished(clientResponse.getObj());
            }

            @Override
            public void exceptionCaught(final ClientResponse<InputStream> clientResponse, final Throwable e) {
                // FIXME collect druid scan bytes instead of http response size
                context.getQueryContext().addAndGetScannedBytes(byteCount.get());

                logger.debug("Failed request for queryId={} intervals={} bytes={} exception={}",
                        query.getId(),
                        query.getIntervals(),
                        byteCount.get(),
                        e.getMessage());

                // Don't wait for lock in case the lock had something to do with the error
                synchronized (done) {
                    done.set(true);
                    // Make a best effort to put a zero length buffer into the queue in case something is waiting on the take()
                    // If nothing is waiting on take(), this will be closed out anyways.
                    queue.offer(
                            new InputStream() {
                                @Override
                                public int read() throws IOException {
                                    throw new IOException(e);
                                }
                            }
                    );
                }
            }
        };

        try {
            future = httpClient.go(
                    new Request(
                            HttpMethod.POST,
                            new URL(url)
                    ).setContent(objectMapper.writeValueAsBytes(query))
                            .setHeader(
                                    HttpHeaders.Names.CONTENT_TYPE,
                                    isSmile ? "application/x-jackson-smile" : "application/json"
                            ),
                    responseHandler
            );

        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return new Cursor<T>() {
            private JsonParser jp;
            private ObjectCodec objectCodec;
            private T next;

            private void init() {
                if (jp == null) {
                    try {
                        jp = objectMapper.getFactory().createParser(future.get());
                        final JsonToken nextToken = jp.nextToken();
                        if (nextToken == JsonToken.START_OBJECT) {
                            QueryInterruptedException cause = jp.getCodec().readValue(jp, QueryInterruptedException.class);
                            throw new QueryInterruptedException(cause, host);
                        } else if (nextToken != JsonToken.START_ARRAY) {
                            throw new IAE("Next token wasn't a START_ARRAY, was[%s] from url [%s]", jp.getCurrentToken(), url);
                        } else {
                            jp.nextToken();
                            objectCodec = jp.getCodec();
                        }
                    } catch (IOException | InterruptedException | ExecutionException e) {
                        throw new RE(e, "Failure getting results from[%s] because of [%s]", url, e.getMessage());
                    } catch (CancellationException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }

            @Override
            public void close() throws IOException {
                if (jp != null) {
                    jp.close();
                }
            }

            @Override
            public boolean hasNext() {
                init();

                if (next != null) {
                    return true;
                }

                if (jp.isClosed()) {
                    return false;
                }
                if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
                    CloseQuietly.close(jp);
                    return false;
                }

                try {
                    next = objectCodec.readValue(jp, typeReference);
                    jp.nextToken();
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                return true;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                T result = next;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        };
    }
}
