package org.safehaus.penrose.ldap.connection;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionListener;
import org.safehaus.penrose.connection.Connection;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPConnection extends Connection {

    public final static String BASE_DN                             = "baseDn";
    public final static String SCOPE                               = "scope";
    public final static String FILTER                              = "filter";
    public final static String OBJECT_CLASSES                      = "objectClasses";
    public final static String SIZE_LIMIT                          = "sizeLimit";
    public final static String TIME_LIMIT                          = "timeLimit";

    public final static String PAGE_SIZE                           = "pageSize";
    public final static int    DEFAULT_PAGE_SIZE                   = 1000;

    public final static String AUTHENTICATION                      = "authentication";
    public final static String AUTHENTICATION_DEFAULT              = "default";
    public final static String AUTHENTICATION_FULL                 = "full";
    public final static String AUTHENTICATION_DISABLED             = "disabled";

    public final static String INITIAL_SIZE                         = "initialSize";
    public final static String MAX_ACTIVE                           = "maxActive";
    public final static String MAX_IDLE                             = "maxIdle";
    public final static String MIN_IDLE                             = "minIdle";
    public final static String MAX_WAIT                             = "maxWait";

    public final static String VALIDATION_QUERY                     = "validationQuery";
    public final static String TEST_ON_BORROW                       = "testOnBorrow";
    public final static String TEST_ON_RETURN                       = "testOnReturn";
    public final static String TEST_WHILE_IDLE                      = "testWhileIdle";
    public final static String TIME_BETWEEN_EVICTION_RUNS_MILLIS    = "timeBetweenEvictionRunsMillis";
    public final static String NUM_TESTS_PER_EVICTION_RUN           = "numTestsPerEvictionRun";
    public final static String MIN_EVICTABLE_IDLE_TIME_MILLIS       = "minEvictableIdleTimeMillis";

    public final static String WHEN_EXHAUSTED_ACTION                = "whenExhaustedAction";
    public final static String WHEN_EXHAUSTED_FAIL                  = "fail";
    public final static String WHEN_EXHAUSTED_BLOCK                 = "block";
    public final static String WHEN_EXHAUSTED_GROW                  = "grow";

    public GenericObjectPool.Config config = new GenericObjectPool.Config();
    public GenericObjectPool connectionPool;

    LDAPConnectionFactory connectionFactory;
    LDAPPoolableConnectionFactory poolableConnectionFactory;
    
    public void init() throws Exception {

        log.debug("Initializing connection "+getName()+".");

        Properties properties = new Properties();
        properties.putAll(getParameters());

        String s = (String)properties.remove(INITIAL_SIZE);
        int initialSize = s == null ? 0 : Integer.parseInt(s);

        s = (String)properties.remove(MAX_ACTIVE);
        if (s != null) config.maxActive = Integer.parseInt(s);

        s = (String)properties.remove(MAX_IDLE);
        if (s != null) config.maxIdle = Integer.parseInt(s);

        s = (String)properties.remove(MAX_WAIT);
        if (s != null) config.maxWait = Long.parseLong(s);

        s = (String)properties.remove(MIN_EVICTABLE_IDLE_TIME_MILLIS);
        if (s != null) config.minEvictableIdleTimeMillis = Integer.parseInt(s);

        s = (String)properties.remove(MIN_IDLE);
        if (s != null) config.minIdle = Integer.parseInt(s);

        s = (String)properties.remove(NUM_TESTS_PER_EVICTION_RUN);
        if (s != null) config.numTestsPerEvictionRun = Integer.parseInt(s);

        s = (String)properties.remove(TEST_ON_BORROW);
        if (s != null) config.testOnBorrow = Boolean.valueOf(s);

        s = (String)properties.remove(TEST_ON_RETURN);
        if (s != null) config.testOnReturn = Boolean.valueOf(s);

        s = (String)properties.remove(TEST_WHILE_IDLE);
        if (s != null) config.testWhileIdle = Boolean.valueOf(s);

        s = (String)properties.remove(TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        if (s != null) config.timeBetweenEvictionRunsMillis = Integer.parseInt(s);

        //s = (String)properties.remove(SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        //if (s != null) config.softMinEvictableIdleTimeMillis = Integer.parseInt(s);

        s = (String)properties.remove(WHEN_EXHAUSTED_ACTION);
        if (WHEN_EXHAUSTED_FAIL.equals(s)) {
            config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_FAIL;

        } else if (WHEN_EXHAUSTED_BLOCK.equals(s)) {
            config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;

        } else if (WHEN_EXHAUSTED_GROW.equals(s)) {
            config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;

        } else {
            config.whenExhaustedAction = GenericObjectPool.DEFAULT_WHEN_EXHAUSTED_ACTION;
        }

        connectionPool = new GenericObjectPool(null, config);

        connectionFactory = new LDAPConnectionFactory(getParameters());

        poolableConnectionFactory = new LDAPPoolableConnectionFactory(
                connectionFactory
        );

        connectionPool.setFactory(poolableConnectionFactory);

        for (int i = 0; i < initialSize; i++) {
             connectionPool.addObject();
         }

        log.debug("Connection "+getName()+" initialized.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Client
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public LDAPClient createClient() throws Exception {

        if (debug) log.debug("Creating new LDAP client.");

        return new LDAPPoolableClient(connectionPool, connectionFactory);
    }

    public LDAPClient getClient(final Session session) throws Exception {

        if (debug) log.debug("Getting LDAP client from session.");
        final String attributeName = getPartition().getName()+".connection."+getName();

        LDAPClient client = (LDAPClient)session.getAttribute(attributeName);
        if (client != null) return client;

        final LDAPClient newClient = createClient();

        if (debug) log.debug("Storing LDAP client in session.");
        session.setAttribute(attributeName, newClient);

        session.addListener(new SessionListener() {
            public void sessionClosed() throws Exception {

                if (debug) log.debug("Closing LDAP client.");

                session.removeAttribute(attributeName);
                newClient.close();
            }
        });

        return newClient;
    }

    public void closeClient(Session session) throws Exception {
    }
}
