package org.safehaus.penrose.ldap;

import org.apache.commons.pool.ObjectPool;
import org.ietf.ldap.LDAPConnection;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPPoolableClient extends LDAPClient {

    ObjectPool objectPool;

    public LDAPPoolableClient(
            ObjectPool objectPool,
            LDAPConnectionFactory connectionFactory
    ) throws Exception {
        super(connectionFactory);
        this.objectPool = objectPool;
        connect();
    }

    public void connect() throws Exception {

        if (connection == null) {
            log.debug("Getting LDAP connection from connection pool.");
            connection = (LDAPConnection)objectPool.borrowObject();
            initConnection();
            return;
        }

        super.connect();
    }

    public void close() throws Exception {
        log.debug("Returning LDAP connection to connection pool.");
        objectPool.returnObject(connection);
    }
}
