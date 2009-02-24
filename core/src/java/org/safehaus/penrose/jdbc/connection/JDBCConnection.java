package org.safehaus.penrose.jdbc.connection;

import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionListener;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.util.TextUtil;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class JDBCConnection extends Connection {

    public final static String DRIVER                               = "driver";
    public final static String URL                                  = "url";
    public final static String USER                                 = "user";
    public final static String PASSWORD                             = "password";
    public final static String QUOTE                                = "quote";
    public final static String QUERY_TIMEOUT                        = "queryTimeout";

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

    public JDBCConnectionFactory connectionFactory;
    public PoolableConnectionFactory poolableConnectionFactory;

    public void init() throws Exception {

        log.debug("Initializing connection "+getName()+".");

        Map<String,String> parameters = new HashMap<String,String>();
        parameters.putAll(getParameters());

        String driverClass = parameters.remove(DRIVER);

        ClassLoader cl = connectionContext.getClassLoader();
        if (cl == null) cl = getClass().getClassLoader();

        Class clazz = cl.loadClass(driverClass);

        Driver driver = (Driver)clazz.newInstance();
        DriverManager.registerDriver(driver);

        String s = parameters.remove(INITIAL_SIZE);
        int initialSize = s == null ? 0 : Integer.parseInt(s);

        s = parameters.remove(MAX_ACTIVE);
        if (s != null) config.maxActive = Integer.parseInt(s);

        s = parameters.remove(MAX_IDLE);
        if (s != null) config.maxIdle = Integer.parseInt(s);

        s = parameters.remove(MIN_IDLE);
        if (s != null) config.minIdle = Integer.parseInt(s);

        s = parameters.remove(MAX_WAIT);
        if (s != null) config.maxWait = Integer.parseInt(s);

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

        String validationQuery = parameters.remove(VALIDATION_QUERY);

        connectionPool = new GenericObjectPool(null, config);

        connectionFactory = new JDBCConnectionFactory(driver, parameters);
        poolableConnectionFactory = new PoolableConnectionFactory(
                connectionFactory,
                connectionPool,
                null, // statement pool factory
                validationQuery, // test query
                false, // read only
                true // auto commit
        );

        connectionPool.setFactory(poolableConnectionFactory);

        //log.debug("Initializing "+initialSize+" connections.");
        for (int i = 0; i < initialSize; i++) {
             connectionPool.addObject();
         }

        log.debug("Connection "+getName()+" initialized.");
    }

    public void destroy() throws Exception {
        connectionPool.close();
        log.debug("Connection "+getName()+" closed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Client
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validate() throws Exception {
        try {
            JDBCClient client = createClient();
            client.connect();
            client.close();
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    public JDBCClient createClient() throws Exception {

        if (debug) log.debug("Creating new JDBC client.");

        return new JDBCPoolableClient(connectionPool, connectionFactory);
    }

    public synchronized JDBCClient getClient(final Session session) throws Exception {

        if (debug) log.debug("Getting LDAP client from session.");
        final String attributeName = getPartition().getName()+".connection."+getName();

        JDBCClient client = (JDBCClient)session.getAttribute(attributeName);
        if (client != null) return client;

        final JDBCClient newClient = createClient();

        if (debug) log.debug("Storing JDBC client in session.");
        session.setAttribute(attributeName, newClient);

        session.addListener(new SessionListener() {
            public void sessionClosed() throws Exception {

                if (debug) log.debug("Closing JDBC client.");

                session.removeAttribute(attributeName);
                newClient.close();
            }
        });

        return newClient;
/*
        String authentication = source.getParameter(AUTHENTICATION);
        if (debug) log.debug("Authentication: "+authentication);

        Partition partition = connectionContext.getPartition();

        Connection connection = source.getConnection();
        JDBCClient client;

        if (AUTHENTICATION_FULL.equals(authentication)) {
            if (debug) log.debug("Getting connection info from session.");

            client = session == null ? null : (JDBCClient)session.getAttribute(partition.getName()+".connection."+connection.getName());

            if (client == null) {

                if (session == null || session.isRootUser()) {
                    if (debug) log.debug("Creating new connection.");

                    client = getClient();
                    //client = new JDBCClient(driver, connection.getParameters());

                } else {
                    if (debug) log.debug("Missing credentials.");
                    throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
                }
            }

        } else {
            client = getClient();
            //client = this.client;
        }

        return client;
*/
    }

    public void closeClient(Session session) throws Exception {

        //String authentication = source.getParameter(AUTHENTICATON);
        //if (debug) log.debug("Authentication: "+authentication);

        //client.close();
    }

    public Collection<String> getCatalogs() throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Get Catalog", 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = createClient();

        try {
            return client.getCatalogs();

        } finally {
            client.close();
        }
    }
    
    public Collection<String> getSchemas() throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Get Schemas", 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = createClient();

        try {
            return client.getSchemas();

        } finally {
            client.close();
        }
    }
    
    public Collection<Table> getTables(String catalog, String schema) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Get Tables for "+catalog+"."+schema, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = createClient();

        try {
            return client.getTables(catalog, schema);

        } finally {
            client.close();
        }
    }
    
    public Collection<FieldConfig> getColumns(String catalog, String schema, String table) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Get Columns for "+catalog+"."+schema+"."+table, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = createClient();

        try {
            return client.getColumns(catalog, schema, table);

        } finally {
            client.close();
        }
    }

    public String getTableName(SourceConfig sourceConfig) throws Exception {

        String catalog = sourceConfig.getParameter(JDBC.CATALOG);
        String schema = sourceConfig.getParameter(JDBC.SCHEMA);
        String table = sourceConfig.getParameter(JDBC.TABLE);

        StringBuilder sb = new StringBuilder();

        if (catalog != null) {
            sb.append(connectionFactory.quote(catalog));
            sb.append(".");
        }

        if (schema != null) {
            sb.append(connectionFactory.quote(schema));
            sb.append(".");
        }

        sb.append(connectionFactory.quote(table));

        return sb.toString();
    }

    public boolean checkDatabase(String database) throws Exception {
        try {
            createDatabase(database);
            dropDatabase(database);
            return false;

        } catch (Exception e) {
            return true;
        }
    }

    public void createDatabase(String database) throws Exception {
        JDBCClient client = createClient();
        client.executeUpdate("create database "+database);
        client.close();
    }
    
    public void dropDatabase(String database) throws Exception {
        JDBCClient client = createClient();
        client.executeUpdate("drop database "+database);
        client.close();
    }

    public void executeQuery(String sql, QueryResponse response) throws Exception {
        JDBCClient client = createClient();
        client.executeQuery(sql, response);
        client.close();
    }
    
    public void executeQuery(
            String sql,
            Object[] parameters,
            QueryResponse queryResponse
    ) throws Exception {
        JDBCClient client = createClient();
        client.executeQuery(sql, Arrays.asList(parameters), queryResponse);
        client.close();
    }

    public int executeUpdate(
            String sql,
            Object[] parameters
    ) throws Exception {
        JDBCClient client = createClient();
        int count = client.executeUpdate(sql, Arrays.asList(parameters));
        client.close();
        return count;
    }
}
