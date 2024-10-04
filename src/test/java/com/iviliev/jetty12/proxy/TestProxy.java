package com.iviliev.jetty12.proxy;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ReadListener;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.ee8.proxy.AsyncMiddleManServlet;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpHeaderValue;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests that a client will receive each separate chunk during a chunked transfer encoding, while the server is
 * streaming them, before the response is completed. Some of this test code has been borrowed from jetty's
 * AsyncMiddleManServletTest.
 */
public abstract class TestProxy {
    private static final Logger testLog = LoggerFactory.getLogger(TestProxy.class);
    private static final Logger proxyLog =
            LoggerFactory.getLogger(TestProxy.class.getPackageName() + ".AsyncMiddleManServlet");
    private static final Logger serverLog = LoggerFactory.getLogger(TestProxy.class.getPackageName() + ".Server");

    protected static final int COUNT_MAX = 5;
    protected static final int COUNT_INTERVAL_SEC = 2;

    private static final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(5);

    protected HttpClient client;
    protected Server proxy;
    protected ServerConnector proxyConnector;
    protected Server server;
    protected ServerConnector serverConnector;

    @AfterEach
    public void dispose() throws Exception {
        testLog.debug("Stopping client");
        client.stop();
        testLog.debug("Stopping proxy");
        proxy.stop();
        testLog.debug("Stopping server");
        server.stop();
    }

    @Test
    public void testChunkedSyncBackendTransfer() throws Exception {
        testChunkedTransfer(new SyncBackendServlet());
    }

    @Test
    public void testChunkedAsyncBackendTransfer() throws Exception {
        testChunkedTransfer(new AsyncBackendServlet());
    }

    @Test
    public void testChunkedTransferAbort() throws Exception {
        // backend
        startServer(new SyncBackendServlet(50));
        AtomicReference<Instant> lastChunkTs = new AtomicReference<>();
        //        startProxy(new AsyncMiddleManServlet());
        startProxy(new MyAMMS(lastChunkTs));
        startClient();

        Request req = initClientRequest();

        ScheduledFuture<?> abort = scheduler.schedule(() -> {
            testLog.debug("Aborting");
            if (!req.abort(new CancelRequest()).join()) {
                throw new IllegalStateException("Could not abort");
            }
            testLog.debug("Aborted successfully");
        }, 3, TimeUnit.SECONDS);

        try {
            ContentResponse response = req.send();
            testLog.error(response.getContentAsString());
        } catch (ExecutionException e) {
            assertInstanceOf(CancelRequest.class, e.getCause());
            assertTrue(abort.isDone());
            abort.get();
            TimeUnit.SECONDS.sleep(10);//give time for the proxy to stop the proxy client
            Instant lastChunkTsSnapshot = lastChunkTs.get();
            TimeUnit.SECONDS.sleep(COUNT_INTERVAL_SEC * 2L);
            assertEquals(lastChunkTsSnapshot, lastChunkTs.get());
            return;
        }

        fail("The request should have failed");
    }

    public void testChunkedTransfer(HttpServlet backend) throws Exception {
        // backend
        startServer(backend);

        //        startProxy(new AsyncMiddleManServlet());
        startProxy(new MyAMMS());
        startClient();

        Semaphore monitor = new Semaphore(0);
        CompletableFuture<Response> result = new CompletableFuture<>();
        AtomicInteger received = new AtomicInteger();
        initClientRequest()
                .onResponseContent((request, content) -> {
                    int c = content.get();
                    testLog.debug("Got {}", c);
                    received.accumulateAndGet(c, Integer::sum);
                    monitor.release();
                })
                .send(r1 -> {
                    if (r1.isFailed()) {
                        result.completeExceptionally(r1.getFailure());
                    } else if (r1.isSucceeded()) {
                        result.complete(r1.getResponse());
                    }
                });
        monitor(received, monitor, result);
        Response response = result.get(COUNT_MAX * COUNT_INTERVAL_SEC * 2L, TimeUnit.SECONDS);
        assertEquals(200, response.getStatus());
        assertEquals(COUNT_MAX * (1 + COUNT_MAX) / 2, received.get());
    }

    protected abstract String getClientRequestUrl();

    private Request initClientRequest() {
        return client
                .newRequest(getClientRequestUrl())
                .idleTimeout(COUNT_INTERVAL_SEC + 1, TimeUnit.SECONDS)
                // simulating a very long-lived response(for example following k8s pod logs)
                .timeout(1, TimeUnit.HOURS)
                .onResponseBegin(response -> testLog.debug("Response version: {}", response.getVersion()))
                .onResponseHeader((response, field) -> {
                    testLog.debug("HttpField: {}", field);
                    return true;
                });
    }

    private static void monitor(AtomicInteger id, Semaphore monitor, CompletableFuture<Response> result) {
        scheduler.schedule(() -> {
            try {
                if (!result.isDone() && monitor.tryAcquire()) {
                    monitor(id, monitor, result);
                } else {
                    result.completeExceptionally(
                            new IllegalStateException("Timed out waiting for next count at: " + id.get()));
                }
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        }, COUNT_INTERVAL_SEC + 1, TimeUnit.SECONDS);
    }

    private void startServer(HttpServlet servlet) throws Exception {
        QueuedThreadPool serverPool = new QueuedThreadPool();
        serverPool.setName("server");
        server = new Server(serverPool);
        serverConnector = initServerConnector(server);

        ServletContextHandler appCtx = new ServletContextHandler();
        appCtx.setContextPath("/");
        server.setHandler(appCtx);
        ServletHolder appServletHolder = new ServletHolder(servlet);
        appCtx.addServlet(appServletHolder, "/*");
        appServletHolder.setAsyncSupported(true);

        server.start();
        testLog.debug("Server port: {}", serverConnector.getLocalPort());
    }

    private void startProxy(AsyncMiddleManServlet proxyServlet) throws Exception {
        QueuedThreadPool proxyPool = new QueuedThreadPool();
        proxyPool.setName("proxy");
        proxy = new Server(proxyPool);
        proxyConnector = initProxyConnector(proxy);
        proxy.addConnector(proxyConnector);

        ServletContextHandler proxyContext = new ServletContextHandler();
        proxyContext.setContextPath("/");
        proxy.setHandler(proxyContext);
        ServletHolder proxyServletHolder = new ServletHolder(proxyServlet);
        proxyServletHolder.setInitParameters(Map.of(
                "idleTimeout", Long.toString(TimeUnit.SECONDS.toMillis(COUNT_INTERVAL_SEC + 1)),
                "timeout", Long.toString(TimeUnit.HOURS.toMillis(1))
        ));
        proxyServletHolder.setAsyncSupported(true);
        proxyContext.addServlet(proxyServletHolder, "/*");

        proxy.start();

        testLog.debug("Proxy port: {}", proxyConnector.getLocalPort());
    }

    private void startClient() throws Exception {
        QueuedThreadPool clientPool = new QueuedThreadPool();
        clientPool.setName("client");
        client = initClient();
        client.setExecutor(clientPool);
        client.start();
    }

    protected abstract ServerConnector initServerConnector(Server server);

    protected abstract ServerConnector initProxyConnector(Server proxy);

    protected abstract HttpClient initClient();

    protected abstract HttpClient initProxyClient();

    protected abstract String rewriteTarget(HttpServletRequest original);

    private static final class AsyncBackendServlet extends HttpServlet {
        private static class Responder implements WriteListener {
            private final AsyncContext ac;
            private final ServletOutputStream os;
            private final AtomicInteger counter = new AtomicInteger();
            private final Semaphore dataGate = new Semaphore(0);

            private Responder(AsyncContext ac) throws IOException {
                this.ac = ac;
                this.ac.setTimeout(0);
                this.os = ac.getResponse().getOutputStream();
                this.os.setWriteListener(this);
                emit();
            }

            @Override
            public void onWritePossible() throws IOException {
                writeOut();
            }

            @Override
            public void onError(Throwable t) {
                serverLog.error("Error during write", t);
                ac.complete();
            }

            private void emit() {
                scheduler.schedule(() -> {
                    try {
                        serverLog.debug("Emit");
                        dataGate.release();
                        writeOut();
                        emit();
                    } catch (Exception e) {
                        serverLog.error("Could not emit", e);
                        try {
                            throw new RuntimeException(e);
                        } finally {
                            ac.complete();
                        }
                    }
                }, COUNT_INTERVAL_SEC, TimeUnit.SECONDS);
            }

            private void writeOut() throws IOException {
                while (os.isReady() && dataGate.tryAcquire()) {
                    int c = counter.incrementAndGet();
                    os.write(c);
                    if (os.isReady()) {
                        os.flush();
                    }
                    if (c >= COUNT_MAX) {
                        ac.complete();
                        return;
                    }
                }
            }
        }

        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException {
            response.addHeader(HttpHeader.TRANSFER_ENCODING.asString(), HttpHeaderValue.CHUNKED.asString());
            AsyncContext ac = request.startAsync();
            new Responder(ac);
        }
    }

    private static final class SyncBackendServlet extends HttpServlet {
        private final int numChunksToSend;

        public SyncBackendServlet(int numChunksToSend) {
            this.numChunksToSend = numChunksToSend;
        }

        public SyncBackendServlet() {
            this(COUNT_MAX);
        }

        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response) {
            response.addHeader(HttpHeader.TRANSFER_ENCODING.asString(), HttpHeaderValue.CHUNKED.asString());
            IntStream.range(1, numChunksToSend + 1).forEach(i -> {
                try {
                    response.getOutputStream().write(i);
                    response.getOutputStream().flush();
                    serverLog.debug("Emit {}", i);
                    TimeUnit.SECONDS.sleep(COUNT_INTERVAL_SEC);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private class MyAMMS extends AsyncMiddleManServlet {
        private final AtomicReference<Instant> lastChunkTs;

        private MyAMMS() {this(null);}

        private MyAMMS(AtomicReference<Instant> lastChunkTs) {this.lastChunkTs = lastChunkTs;}

        @Override
        protected Logger createLogger() {
            return proxyLog;
        }

        @Override
        protected String rewriteTarget(HttpServletRequest clientRequest) {
            String target = TestProxy.this.rewriteTarget(clientRequest);
            if (target == null) {
                return super.rewriteTarget(clientRequest);
            }
            return target;
        }

        @Override
        protected void sendProxyRequest(HttpServletRequest clientRequest, HttpServletResponse proxyResponse,
                                        Request proxyRequest) {

            AsyncContext ctx = clientRequest.getAsyncContext();
            AsyncCallback callback = new AsyncCallback(proxyRequest);
            try {
                // need to override ProxyReader and ProxyWriter instead
                //clientRequest.getInputStream().setReadListener(callback);
                //proxyResponse.getOutputStream().setWriteListener(callback);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            ctx.addListener(callback);
            super.sendProxyRequest(clientRequest, proxyResponse, proxyRequest);
        }

        @Override
        protected HttpClient newHttpClient() {
            return TestProxy.this.initProxyClient();
        }

        @Override
        protected AsyncMiddleManServlet.ProxyWriter newProxyWriteListener(HttpServletRequest clientRequest,
                                                                          Response proxyResponse) {
            return new AsyncMiddleManServlet.ProxyWriter(clientRequest, proxyResponse) {
                @Override
                public void onWritePossible() throws IOException {
                    super.onWritePossible();
                    if (lastChunkTs != null) {
                        lastChunkTs.set(Instant.now());
                    }
                    ServletOutputStream output = clientRequest.getAsyncContext().getResponse().getOutputStream();
                    // FIXME how else do we fix this
//                    if (output.isReady()) {
//                        output.flush();
//                    }
                }

                // this is a proxy response write error callback
                @Override
                public void onError(Throwable failure) {
                    proxyLog.trace("Proxy response output stream error", failure);
                    super.onError(failure);
                }
            };
        }
    }

    private static class AsyncCallback implements AsyncListener, ReadListener, WriteListener {
        private final Request proxyRequest;
        private final AtomicReference<CancellationException> abort = new AtomicReference<>();

        private AsyncCallback(Request proxyRequest) {this.proxyRequest = proxyRequest;}

        private void end(Throwable event) {
            proxyLog.debug("End callback: {}", event == null ? "cancelled" : event);
            if (true) {
                return;
            }
            CancellationException e = new CancellationException();
            if (event != null) {
                e.addSuppressed(event);
            }
            if (abort.compareAndSet(null, e)) {
                proxyLog.debug("Aborting", abort.get());
                proxyRequest
                        .abort(abort.get())
                        .whenComplete((r, t) -> {
                            if (t != null) {
                                proxyLog.error("Could not abort", t);
                            } else if (!r) {
                                proxyLog.debug("Could not abort the proxy request={}", proxyRequest.toString());
                            } else {
                                proxyLog.debug("Aborted request={}", proxyRequest.toString());
                            }
                        });
            }
        }

        @Override
        public void onComplete(AsyncEvent event) throws IOException {
            end(event.getThrowable());
        }

        @Override
        public void onTimeout(AsyncEvent event) throws IOException {

        }

        @Override
        public void onError(AsyncEvent event) throws IOException {
            end(event.getThrowable());
        }

        @Override
        public void onStartAsync(AsyncEvent event) throws IOException {

        }

        @Override
        public void onDataAvailable() throws IOException {

        }

        @Override
        public void onAllDataRead() throws IOException {

        }

        @Override
        public void onWritePossible() throws IOException {

        }

        @Override
        public void onError(Throwable t) {
            end(t);
        }
    }

    private static class CancelRequest extends RuntimeException {
    }
}
