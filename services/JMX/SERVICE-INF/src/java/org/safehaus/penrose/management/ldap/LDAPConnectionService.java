package org.safehaus.penrose.management.ldap;

import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.management.connection.ConnectionService;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.connection.LDAPConnection;
import org.safehaus.penrose.ldap.connection.LDAPConnectionServiceMBean;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.schema.Schema;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPConnectionService extends ConnectionService implements LDAPConnectionServiceMBean {

    public LDAPConnectionService(
            PenroseJMXService jmxService,
            PartitionManager partitionManager,
            String partitionName,
            String connectionName
    ) throws Exception {
        super(jmxService, partitionManager, partitionName, connectionName);
    }

    public SearchResult find(String dn) throws Exception {
        Session session = createAdminSession();
        LDAPClient client = null;

        try {
            LDAPConnection connection = (LDAPConnection)getConnection();
            client = connection.getClient(session);
            return client.find(dn);

        } finally {
            if (client != null) try { client.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (session != null) try { session.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }
    }

    public SearchResult find(DN dn) throws Exception {
        Session session = createAdminSession();
        LDAPClient client = null;

        try {
            LDAPConnection connection = (LDAPConnection)getConnection();
            client = connection.getClient(session);
            return client.find(dn);

        } finally {
            if (client != null) try { client.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (session != null) try { session.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }
    }

    public SearchResponse search(SearchRequest request, SearchResponse response) throws Exception {
        Session session = createAdminSession();
        LDAPClient client = null;

        try {
            LDAPConnection connection = (LDAPConnection)getConnection();
            client = connection.getClient(session);
            client.search(request, response);

            int rc = response.waitFor();
            log.debug("RC: "+rc);

        } catch (Exception e) {
            response.setException(e);
            
        } finally {
            if (client != null) try { client.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (session != null) try { session.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return response;
    }

    public Schema getSchema() throws Exception {
        LDAPConnection connection = (LDAPConnection)getConnection();
        return connection.getSchema();
    }
}