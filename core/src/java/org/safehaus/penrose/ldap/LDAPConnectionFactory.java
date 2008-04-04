package org.safehaus.penrose.ldap;

import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPUrl;
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

    public LDAPConnectionFactory(Map<String,?> parameters) throws Exception {
        init();
        parseParameters(parameters);
    }

    public LDAPConnectionFactory(String url) throws Exception {
        init();
        parseUrl(url);
    }

    public LDAPConnectionFactory(LDAPUrl url) throws Exception {
        init();
        urls.add(url);
    }

    public LDAPConnectionFactory(Collection<LDAPUrl> urls) throws Exception {
        init();
        this.urls = urls;
    }

    public void init() {
        for (String attribute : LDAPClient.DEFAULT_BINARY_ATTRIBUTES) {
            binaryAttributes.add(attribute.toLowerCase());
        }
    }

    public void parseParameters(Map<String,?> parameters) throws Exception {

        String url = (String)parameters.get(Context.PROVIDER_URL);
        parseUrl(url);

        bindDn = (String)parameters.get(Context.SECURITY_PRINCIPAL);

        String stringPassword = (String)parameters.get(Context.SECURITY_CREDENTIALS);
        if (stringPassword != null) {
            bindPassword = stringPassword.getBytes("UTF-8");
        }

        String s = (String)parameters.get(Context.REFERRAL);
        if (s != null) {
            referral = s;
        }

        s = (String)parameters.get("java.naming.ldap.attributes.binary");
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

    public LDAPConnection createConnection() throws Exception {

        log.debug("Creating LDAP connection...");

        LDAPConnection connection = new LDAPConnection();
        connect(connection);

        return connection;
    }

    public void connect(LDAPConnection connection) throws Exception {
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
