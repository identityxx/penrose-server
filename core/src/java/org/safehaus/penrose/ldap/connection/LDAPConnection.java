package org.safehaus.penrose.ldap.connection;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionListener;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.schema.SchemaUtil;
import org.safehaus.penrose.schema.Schema;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPConnection extends Connection {

    public final static String URL       = "url";
    public final static String USER      = "user";
    public final static String PASSWORD  = "password";
    public final static String REFERRAL  = "referral";
    public final static String PAGE_SIZE = "pageSize";
    public final static String TIMEOUT   = "timeout"; // millisecond

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

    public final static String SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS  = "softMinEvictableIdleTimeMillis";
    public final static String WHEN_EXHAUSTED_ACTION                = "whenExhaustedAction";
    public final static String WHEN_EXHAUSTED_FAIL                  = "fail";
    public final static String WHEN_EXHAUSTED_BLOCK                 = "block";
    public final static String WHEN_EXHAUSTED_GROW                  = "grow";

    public GenericObjectPool.Config config = new GenericObjectPool.Config();
    public GenericObjectPool connectionPool;

    public LDAPConnectionFactory connectionFactory;
    public LDAPPoolableConnectionFactory poolableConnectionFactory;
    
    public void init() throws Exception {

        log.debug("Initializing connection "+getName()+".");

        Map<String,String> parameters = new HashMap<String,String>();
        parameters.putAll(getParameters());

        String s = parameters.remove(INITIAL_SIZE);
        int initialSize = s == null ? 0 : Integer.parseInt(s);

        s = parameters.remove(MAX_ACTIVE);
        if (s != null) config.maxActive = Integer.parseInt(s);

        s = parameters.remove(MAX_IDLE);
        if (s != null) config.maxIdle = Integer.parseInt(s);

        s = parameters.remove(MIN_IDLE);
        if (s != null) config.minIdle = Integer.parseInt(s);

        s = parameters.remove(MAX_WAIT);
        if (s != null) config.maxWait = Long.parseLong(s);

        s = parameters.remove(TEST_ON_BORROW);
        if (s != null) config.testOnBorrow = Boolean.valueOf(s);

        s = parameters.remove(TEST_ON_RETURN);
        if (s != null) config.testOnReturn = Boolean.valueOf(s);

        s = parameters.remove(TEST_WHILE_IDLE);
        if (s != null) config.testWhileIdle = Boolean.valueOf(s);

        s = parameters.remove(MIN_EVICTABLE_IDLE_TIME_MILLIS);
        if (s != null) config.minEvictableIdleTimeMillis = Integer.parseInt(s);

        s = parameters.remove(NUM_TESTS_PER_EVICTION_RUN);
        if (s != null) config.numTestsPerEvictionRun = Integer.parseInt(s);

        s = parameters.remove(TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        if (s != null) config.timeBetweenEvictionRunsMillis = Integer.parseInt(s);

        //s = parameters.remove(SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        //if (s != null) config.softMinEvictableIdleTimeMillis = Integer.parseInt(s);

        s = parameters.remove(WHEN_EXHAUSTED_ACTION);
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

        connectionFactory = new LDAPConnectionFactory(parameters);
        poolableConnectionFactory = new LDAPPoolableConnectionFactory(connectionFactory);

        connectionPool.setFactory(poolableConnectionFactory);

        //log.debug("Initializing "+initialSize+" connections.");
        for (int i = 0; i < initialSize; i++) {
             connectionPool.addObject();
         }

        log.debug("Connection "+getName()+" initialized.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Client
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validate() throws Exception {
        LDAPClient client = createClient();
        try {
            client.connect();
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            client.close();
        }
    }

    public Schema getSchema() throws Exception {
        LDAPClient client = createClient();
        try {
            client.connect();

            SchemaUtil schemaUtil = new SchemaUtil();
            return schemaUtil.getSchema(client);

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            client.close();
        }
    }

    public LDAPClient createClient() throws Exception {

        if (debug) log.debug("Creating new LDAP client.");

        return new LDAPPoolableClient(connectionPool, connectionFactory);
    }

    public synchronized LDAPClient getClient(final Session session) throws Exception {

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
