package org.safehaus.penrose.ldap;

import org.ietf.ldap.LDAPException;
import com.novell.ldap.LDAPUrl;
import org.safehaus.penrose.ldap.connection.LDAPSocketFactory;
import org.safehaus.penrose.ldap.connection.LDAPConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPConnectionFactory {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Collection<LDAPUrl> urls = new ArrayList<LDAPUrl>();
    public Collection<String> binaryAttributes = new HashSet<String>();

    public String bindDn;
    public byte[] bindPassword;

    public String referral = "follow";
    public int pageSize;
    public Integer timeout;

    LDAPSocketFactory socketFactory;

    public LDAPConnectionFactory(Map<String,String> parameters) throws Exception {
        parseParameters(parameters);
        init();
    }

    public LDAPConnectionFactory(String url) throws Exception {
        parseUrl(url);
        init();
    }

    public LDAPConnectionFactory(LDAPUrl url) throws Exception {
        urls.add(url);
        init();
    }

    public LDAPConnectionFactory(Collection<LDAPUrl> urls) throws Exception {
        this.urls = urls;
        init();
    }

    public void init() {
        for (String attribute : LDAPClient.DEFAULT_BINARY_ATTRIBUTES) {
            binaryAttributes.add(attribute.toLowerCase());
        }

        socketFactory = new LDAPSocketFactory(urls);
        if (timeout != null) socketFactory.setTimeout(timeout);
    }

    public void parseParameters(Map<String,String> parameters) throws Exception {

        String url = parameters.get(Context.PROVIDER_URL);
        parseUrl(url);

        bindDn = parameters.get(Context.SECURITY_PRINCIPAL);

        String s = parameters.get(Context.SECURITY_CREDENTIALS);
        if (s != null) {
            bindPassword = s.getBytes("UTF-8");
        }

        s = parameters.get(Context.REFERRAL);
        if (s != null) {
            referral = s;
        }

        s = parameters.get(LDAPConnection.PAGE_SIZE);
        if (s != null) {
            pageSize = Integer.parseInt(s);
        }

        s = parameters.get(LDAPConnection.TIMEOUT);
        if (s != null) {
            timeout = Integer.parseInt(s);
        }

        s = parameters.get("java.naming.ldap.attributes.binary");
        if (s != null) {
            StringTokenizer st = new StringTokenizer(s);
            while (st.hasMoreTokens()) {
                String attribute = st.nextToken();
                binaryAttributes.add(attribute.toLowerCase());
            }
        }
    }

    public void parseUrl(String url) throws Exception {
        for (StringTokenizer st = new StringTokenizer(url); st.hasMoreTokens(); ) {
            String token = st.nextToken();
            LDAPUrl ldapUrl = new LDAPUrl(token);
            urls.add(ldapUrl);
        }
    }

    public org.ietf.ldap.LDAPConnection createConnection() throws Exception {

        log.debug("Creating LDAP connection...");

        org.ietf.ldap.LDAPConnection connection = new org.ietf.ldap.LDAPConnection(socketFactory);
        connect(connection);

        return connection;
    }

    public void connect(org.ietf.ldap.LDAPConnection connection) throws Exception {
        for (LDAPUrl url : urls) {
            try {
                connection.connect(url.getHost(), url.getPort());
                log.debug("Connected to "+url+".");
                break;

            } catch (LDAPException e) {
                log.debug("Failed connecting to "+url+".");
            }
        }

        if (!connection.isConnected()) throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }
}
