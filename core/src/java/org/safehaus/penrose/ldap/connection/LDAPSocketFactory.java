package org.safehaus.penrose.ldap.connection;

import com.novell.ldap.LDAPUrl;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPSocketFactory implements org.ietf.ldap.LDAPSocketFactory {

    protected Collection<LDAPUrl> urls;
    protected Integer timeout;

    protected SocketFactory factory;

    public LDAPSocketFactory(Collection<LDAPUrl> urls) {
        this.urls = urls;
        factory = SSLSocketFactory.getDefault();
    }

    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {

        Socket socket = null;

        for (LDAPUrl url : urls) {
            String urlHost = url.getHost();
            int urlPort = url.getPort();

            if (host.equals(urlHost) && port == urlPort) {
                if (url.isSecure()) {
                    socket = createSecureSocket(host, port);
                } else {
                    socket = createRegularSocket(host, port);
                }
            }
        }

        if (socket == null) {
            socket = createRegularSocket(host, port);
        }

        if (timeout != null) socket.setSoTimeout(timeout);

        return socket;
    }

    public Socket createRegularSocket(String host, int port) throws IOException, UnknownHostException {
        return new Socket(host, port);
    }

    public Socket createSecureSocket(String host, int port) throws IOException, UnknownHostException {
        return factory.createSocket(host, port);
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }
}
