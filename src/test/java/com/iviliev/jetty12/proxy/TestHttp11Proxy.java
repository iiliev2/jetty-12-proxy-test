package com.iviliev.jetty12.proxy;

import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

public class TestHttp11Proxy extends TestProxy{
    @Override
    protected String getClientRequestUrl() {
        return new Origin( "http", "localhost", serverConnector.getLocalPort()).asString();
    }

    @Override
    protected String rewriteTarget(HttpServletRequest original) {
        return null;
    }

    @Override
    protected ServerConnector initServerConnector(Server server) {
        ServerConnector serverConnector = new ServerConnector(server);
        server.addConnector(serverConnector);
        return serverConnector;
    }

    @Override
    protected ServerConnector initProxyConnector(Server proxy) {
        HttpConfiguration configuration = new HttpConfiguration();
        configuration.setSendDateHeader(false);
        configuration.setSendServerVersion(false);
        //        configuration.setOutputBufferSize(Integer.parseInt(value));
        ServerConnector proxyConnector = new ServerConnector(proxy, new HttpConnectionFactory(configuration));
        proxyConnector.setIdleTimeout(TimeUnit.SECONDS.toMillis(COUNT_INTERVAL_SEC + 1));
        proxy.addConnector(proxyConnector);
        return proxyConnector;
    }

    @Override
    protected HttpClient initProxyClient() {
        return new HttpClient();
    }

    @Override
    protected HttpClient initClient() {
        HttpClient client = new HttpClient();
        // disabling the proxy also fixes the tests
        client.getProxyConfiguration().addProxy(new HttpProxy("localhost", proxyConnector.getLocalPort()));
        return client;
    }
}
