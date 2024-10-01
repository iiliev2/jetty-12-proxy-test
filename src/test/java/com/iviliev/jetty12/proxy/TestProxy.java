package com.iviliev.jetty12.proxy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.ee8.proxy.AsyncMiddleManServlet;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpHeaderValue;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that a client will receive each separate chunk during a chunked transfer encoding, while the server is
 * streaming them, before the response is completed. Some of this test code has been borrowed from jetty's
 * AsyncMiddleManServletTest.
 */
public class TestProxy {
    private static int COUNT_MAX = 5;
    private static int COUNT_INTERVAL_SEC = 2;

    private static final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(5);

    private HttpClient client;
    private Server proxy;
    private ServerConnector proxyConnector;
    private AsyncMiddleManServlet proxyServlet;
    private Server server;
    private ServerConnector serverConnector;

    @AfterEach
    public void dispose() throws Exception {
        client.stop();
        proxy.stop();
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

    public void testChunkedTransfer(HttpServlet backend) throws Exception {
        // backend
        startServer(backend);

        //        startProxy(new AsyncMiddleManServlet());
        startProxy(new AsyncMiddleManServlet() {
            @Override
            protected ProxyWriter newProxyWriteListener(HttpServletRequest clientRequest, Response proxyResponse) {
                return new ProxyWriter(clientRequest, proxyResponse) {
                    @Override
                    public void onWritePossible() throws IOException {
                        super.onWritePossible();
                        ServletOutputStream output = clientRequest.getAsyncContext().getResponse().getOutputStream();
                        // FIXME how else do we fix this
                        //output.flush();
                    }
                };
            }
        });
        startClient();

        Semaphore monitor = new Semaphore(0);
        CompletableFuture<Response> result = new CompletableFuture<>();
        AtomicInteger received = new AtomicInteger();
        client
                .newRequest("localhost", serverConnector.getLocalPort())
                //.idleTimeout(COUNT_INTERVAL_SEC + 1, TimeUnit.SECONDS)
                // simulating a very long-lived response(for example following k8s pod logs)
                .timeout(1, TimeUnit.HOURS)
                .onResponseBegin(response -> System.out.printf("Response version: %s%n", response.getVersion()))
                .onResponseHeader((response, field) -> {
                    System.out.printf("HttpField: %s%n", field);
                    return false;
                })
                .onResponseContent((request, content) -> {
                    int c = content.get();
                    System.out.printf("Got %d%n", c);
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
        Response response = result.get(COUNT_MAX * COUNT_INTERVAL_SEC * 2, TimeUnit.SECONDS);
        assertEquals(200, response.getStatus());
        assertEquals(COUNT_MAX * (1 + COUNT_MAX) / 2, received.get());
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
        serverConnector = new ServerConnector(server);
        server.addConnector(serverConnector);

        ServletContextHandler appCtx = new ServletContextHandler();
        appCtx.setContextPath("/");
        server.setHandler(appCtx);
        ServletHolder appServletHolder = new ServletHolder(servlet);
        appCtx.addServlet(appServletHolder, "/*");
        appServletHolder.setAsyncSupported(true);

        server.start();
    }

    private void startProxy(AsyncMiddleManServlet proxyServlet) throws Exception {
        QueuedThreadPool proxyPool = new QueuedThreadPool();
        proxyPool.setName("proxy");
        proxy = new Server(proxyPool);

        HttpConfiguration configuration = new HttpConfiguration();
        configuration.setSendDateHeader(false);
        configuration.setSendServerVersion(false);
        //        configuration.setOutputBufferSize(Integer.parseInt(value));
        proxyConnector = new ServerConnector(proxy, new HttpConnectionFactory(configuration));
        proxy.addConnector(proxyConnector);

        ServletContextHandler proxyContext = new ServletContextHandler();
        proxyContext.setContextPath("/");
        proxy.setHandler(proxyContext);
        this.proxyServlet = proxyServlet;
        ServletHolder proxyServletHolder = new ServletHolder(proxyServlet);
        proxyServletHolder.setInitParameters(Map.of());
        proxyServletHolder.setAsyncSupported(true);
        proxyContext.addServlet(proxyServletHolder, "/*");

        proxy.start();
    }

    private void startClient() throws Exception {
        QueuedThreadPool clientPool = new QueuedThreadPool();
        clientPool.setName("client");
        client = new HttpClient();
        client.setExecutor(clientPool);
        // disabling the proxy also fixes the tests
        client.getProxyConfiguration().addProxy(new HttpProxy("localhost", proxyConnector.getLocalPort()));
        client.start();
    }

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
                System.err.println(t.getMessage());
                ac.complete();
            }

            private void emit() {
                scheduler.schedule(() -> {
                    try {
                        System.out.printf("Emit%n");
                        dataGate.release();
                        writeOut();
                        emit();
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
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
        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response) {
            response.addHeader(HttpHeader.TRANSFER_ENCODING.asString(), HttpHeaderValue.CHUNKED.asString());
            IntStream.range(1, COUNT_MAX + 1).forEach(i -> {
                try {
                    response.getOutputStream().write(i);
                    response.getOutputStream().flush();
                    System.out.printf("Emit%n");
                    TimeUnit.SECONDS.sleep(COUNT_INTERVAL_SEC);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

}
