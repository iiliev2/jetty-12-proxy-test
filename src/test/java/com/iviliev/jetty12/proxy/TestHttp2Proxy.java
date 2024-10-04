package com.iviliev.jetty12.proxy;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.transport.HttpClientConnectionFactory;
import org.eclipse.jetty.client.transport.HttpClientTransportDynamic;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.transport.ClientConnectionFactoryOverHTTP2;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.io.ClientConnectionFactory;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class TestHttp2Proxy extends TestProxy {
    private static final Pattern URL =
            Pattern.compile("(?<prefix>https?://localhost:)(?<port>[0-9]+)(?<suffix>/.+|/?)");

    @Override
    protected String getClientRequestUrl() {
        return new Origin("https", "localhost", proxyConnector.getLocalPort()).asString();
    }

    @Override
    protected String rewriteTarget(HttpServletRequest clientRequest) {
        StringBuffer url = clientRequest.getRequestURL();
        if (clientRequest.getQueryString() != null) {
            url.append("?").append(clientRequest.getQueryString());
        }
        Matcher m = URL.matcher(url.toString());
        if (m.matches()) {
            return m.group("prefix") + serverConnector.getLocalPort() + m.group("suffix");
        }
        throw new IllegalArgumentException(clientRequest.getRequestURL().toString());
    }

    @Override
    protected ServerConnector initServerConnector(Server server) {
        return initHTTP2ServerConnector(server, "/server.keystore");
    }

    @Override
    protected ServerConnector initProxyConnector(Server proxy) {
        return initHTTP2ServerConnector(proxy, "/proxy.keystore");
    }

    @Override
    protected HttpClient initClient() {
        ClientConnectionFactory.Info h1 = HttpClientConnectionFactory.HTTP11;
        ClientConnector clientConnector = new ClientConnector();
        SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();
        sslContextFactory.setTrustAll(true);
        clientConnector.setSslContextFactory(sslContextFactory);

        HTTP2Client http2Client = new HTTP2Client(clientConnector);
        ClientConnectionFactory.Info h2 = new ClientConnectionFactoryOverHTTP2.HTTP2(http2Client);

        // See https://github.com/jetty/jetty.project/issues/7575#issuecomment-2126364786 why h1 is before h2
        HttpClientTransport transport = new HttpClientTransportDynamic(clientConnector, h1, h2);
        return new HttpClient(transport);
    }

    @Override
    protected HttpClient initProxyClient() {
        return initClient();
    }

    protected ServerConnector initHTTP2ServerConnector(Server server, String ksName) {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setKeyStorePath(getClass().getResource(ksName).toString());
        sslContextFactory.setKeyStorePassword("password");
        sslContextFactory.setTrustAll(true);

        HttpConfiguration plainConfig = new HttpConfiguration();

        HttpConfiguration secureConfig = new HttpConfiguration(plainConfig);
        secureConfig.addCustomizer(new SecureRequestCustomizer());

        HttpConnectionFactory https = new HttpConnectionFactory(secureConfig);
        HTTP2ServerConnectionFactory http2 = new HTTP2ServerConnectionFactory(secureConfig);
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol(https.getProtocol());
        SslConnectionFactory ssl = new SslConnectionFactory(sslContextFactory, alpn.getProtocol());
        ServerConnector connector = new ServerConnector(server, 1, 1, ssl, alpn, http2, https);
        server.addConnector(connector);
        return connector;
    }
}
