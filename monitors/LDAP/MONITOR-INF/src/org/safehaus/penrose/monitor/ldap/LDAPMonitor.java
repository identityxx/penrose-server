package org.safehaus.penrose.monitor.ldap;

import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPSearchConstraints;
import org.ietf.ldap.LDAPSearchResults;
import org.ietf.ldap.LDAPUrl;
import org.safehaus.penrose.monitor.PollingMonitor;

import java.text.NumberFormat;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPMonitor extends PollingMonitor {

    public final static String URL           = "url";
    public final static String BIND_DN       = "bindDn";
    public final static String BIND_PASSWORD = "bindPassword";

    public final static String SIZE_LIMIT    = "sizeLimit";
    public final static String TIME_LIMIT    = "timeLimit";

    public final static String STARTED       = "STARTED";
    public final static String STOPPED       = "STOPPED";

    public LDAPUrl url;
    public String bindDn;
    public byte[] bindPassword;

    public int sizeLimit;
    public int timeLimit; // milliseconds

    NumberFormat percentFormat = NumberFormat.getPercentInstance();

    public String status = STARTED;

    public void init() throws Exception {

        super.init();

        String s = getParameter(URL);
        log.debug("URL: "+s);
        url = new LDAPUrl(s);
        
        bindDn = getParameter(BIND_DN);
        log.debug("Bind DN: "+bindDn);

        s = getParameter(BIND_PASSWORD);
        log.debug("Bind Password: "+s);
        if (s != null) bindPassword = s.getBytes();

        s = getParameter(SIZE_LIMIT);
        log.debug("Size Limit: "+s);
        if (s != null) sizeLimit = Integer.parseInt(s);

        s = getParameter(TIME_LIMIT);
        log.debug("Time Limit: "+s);
        if (s != null) timeLimit = Integer.parseInt(s);
    }

    public void validate() throws Exception {

        try {
            log.debug("Connecting to "+url+".");
            
            String host = url.getHost();
            int port = url.getPort();
            String baseDn = url.getDN();

            LDAPConnection connection = new LDAPConnection();
            connection.connect(host, port);

            if (bindDn != null && bindPassword != null) {
                log.debug("Binding as "+bindDn+".");
                connection.bind(3, bindDn, bindPassword);
            }

            log.debug("Searching "+baseDn+".");

            LDAPSearchConstraints constraints = new LDAPSearchConstraints();
            constraints.setMaxResults(sizeLimit);
            constraints.setTimeLimit(timeLimit);

            LDAPSearchResults rs = connection.search(
                    baseDn,
                    LDAPConnection.SCOPE_BASE,
                    "(objectClass=*)",
                    new String[0],
                    false,
                    constraints
            );

            if (!rs.hasMore()) {
                throw new Exception("Entry "+baseDn+" is not available.");
            }

            connection.disconnect();

            if (status.equals(STOPPED)) {

                log.warn("Penrose Server is up.");

                status = STARTED;
            }

        } catch (Exception e) {

            if (status.equals(STARTED)) {

                log.warn("Penrose Server is down.");

                log.error(e.getMessage(), e);
                monitorContext.getMonitorManager().restart();

                status = STOPPED;
            }
        }
    }
}
