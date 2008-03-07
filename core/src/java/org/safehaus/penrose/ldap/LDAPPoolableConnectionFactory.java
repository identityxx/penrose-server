package org.safehaus.penrose.ldap;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.ietf.ldap.LDAPConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPPoolableConnectionFactory extends BasePoolableObjectFactory {

    public Logger log = LoggerFactory.getLogger(getClass());

    LDAPConnectionFactory connectionFactory;

    public LDAPPoolableConnectionFactory(
            LDAPConnectionFactory connectionFactory
    ) {
        this.connectionFactory = connectionFactory;
    }

    public Object makeObject() throws Exception {

        log.debug("Creating pooled LDAP connection.");

        return connectionFactory.createConnection();
    }

    public void destroyObject(Object object) throws Exception {

        log.debug("Destroying pooled LDAP connection.");

        LDAPConnection connection = (LDAPConnection)object;
        connection.disconnect();
    }

    public boolean validateObject(Object object) {

        //log.debug("Validating pooled LDAP connection.");

        LDAPConnection connection = (LDAPConnection)object;
        return connection.isConnected();
    }

    public void activateObject(Object object) throws Exception {

        //log.debug("Activating pooled LDAP connection.");

        LDAPConnection connection = (LDAPConnection)object;
        if (connection.isBound()) {
            connection.bind(3, null, null);
        }
    }

    public void passivateObject(Object object) throws Exception {

        //log.debug("Passivating pooled LDAP connection.");

        LDAPConnection connection = (LDAPConnection)object;
        if (connection.isBound()) {
            connection.bind(3, null, null);
        }
    }
}
