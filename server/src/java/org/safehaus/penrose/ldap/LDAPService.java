package org.safehaus.penrose.ldap;

import org.safehaus.penrose.service.Service;

/**
 * @author Endi S. Dewata
 */
public class LDAPService extends Service {

    public final static String LDAP_PORT             = "ldapPort";
    public final static int DEFAULT_LDAP_PORT        = 10389;

    public final static String ENABLE_LDAPS          = "enableLdaps";
    public final static boolean DEFAULT_ENABLE_LDAPS = false;

    public final static String LDAPS_PORT            = "ldapsPort";
    public final static int DEFAULT_LDAPS_PORT       = 10636;

    public final static String LDAPS_CERTIFICATE_FILE     = "ldapsCertificateFile";
    public final static String LDAPS_CERTIFICATE_PASSWORD = "ldapsCertificatePassword";

    public final static String ALLOW_ANONYMOUS_ACCESS          = "allowAnonymousAccess";
    public final static boolean DEFAULT_ALLOW_ANONYMOUS_ACCESS = true;

    public final static String MAX_THREADS           = "maxThreads";
    public final static int DEFAULT_MAX_THREADS      = 20;

    protected int ldapPort;

    protected boolean enableLdaps;
    protected int ldapsPort;

    protected String ldapsCertificateFile;
    protected String ldapsCertificatePassword;

    protected boolean allowAnonymousAccess;

    protected int maxThreads;

    public int getLdapPort() {
        return ldapPort;
    }

    public void setLdapPort(int ldapPort) {
        this.ldapPort = ldapPort;
    }

    public int getLdapsPort() {
        return ldapsPort;
    }

    public void setLdapsPort(int ldapsPort) {
        this.ldapsPort = ldapsPort;
    }

    public boolean isAllowAnonymousAccess() {
        return allowAnonymousAccess;
    }

    public void setAllowAnonymousAccess(boolean allowAnonymousAccess) {
        this.allowAnonymousAccess = allowAnonymousAccess;
    }

    public boolean isEnableLdaps() {
        return enableLdaps;
    }

    public void setEnableLdaps(boolean enableLdaps) {
        this.enableLdaps = enableLdaps;
    }

    public String getLdapsCertificateFile() {
        return ldapsCertificateFile;
    }

    public void setLdapsCertificateFile(String ldapsCertificateFile) {
        this.ldapsCertificateFile = ldapsCertificateFile;
    }

    public String getLdapsCertificatePassword() {
        return ldapsCertificatePassword;
    }

    public void setLdapsCertificatePassword(String ldapsCertificatePassword) {
        this.ldapsCertificatePassword = ldapsCertificatePassword;
    }

    public void init() throws Exception {

        String s = getParameter(LDAP_PORT);
        ldapPort = s == null ? DEFAULT_LDAP_PORT : Integer.parseInt(s);

        s = getParameter(ENABLE_LDAPS);
        enableLdaps = s == null ? DEFAULT_ENABLE_LDAPS : new Boolean(s).booleanValue();

        s = getParameter(LDAPS_PORT);
        ldapsPort = s == null ? DEFAULT_LDAPS_PORT : Integer.parseInt(s);

        ldapsCertificateFile = getParameter(LDAPS_CERTIFICATE_FILE);
        ldapsCertificatePassword = getParameter(LDAPS_CERTIFICATE_PASSWORD);

        s = getParameter(ALLOW_ANONYMOUS_ACCESS);
        allowAnonymousAccess = s == null ? DEFAULT_ALLOW_ANONYMOUS_ACCESS : Boolean.valueOf(s).booleanValue();

        s = getParameter(MAX_THREADS);
        maxThreads = s == null ? DEFAULT_MAX_THREADS : Integer.parseInt(s);
    }
}
