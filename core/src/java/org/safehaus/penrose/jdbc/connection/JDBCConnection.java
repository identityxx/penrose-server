package org.safehaus.penrose.jdbc.connection;

import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.jdbc.source.JDBCSource;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;

import javax.sql.DataSource;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class JDBCConnection extends Connection {

    public final static String DRIVER       = "driver";
    public final static String URL          = "url";
    public final static String USER         = "user";
    public final static String PASSWORD     = "password";
    public final static String QUOTE        = "quote";

    public final static String BASE_DN      = "baseDn";
    public final static String CATALOG      = "catalog";
    public final static String SCHEMA       = "schema";
    public final static String TABLE        = "table";
    public final static String FILTER       = "filter";
    public final static String SIZE_LIMIT   = "sizeLimit";

    public final static String AUTHENTICATION          = "authentication";
    public final static String AUTHENTICATION_DEFAULT  = "default";
    public final static String AUTHENTICATION_FULL     = "full";
    public final static String AUTHENTICATION_DISABLED = "disabled";

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

    public GenericObjectPool.Config config = new GenericObjectPool.Config();
    public GenericObjectPool connectionPool;
    public DataSource ds;

    private Driver driver;
    private JDBCClient client;

    public void init() throws Exception {

        Properties properties = new Properties();
        properties.putAll(getParameters());

        String driverClass = (String)properties.remove(DRIVER);
        String url = (String)properties.remove(URL);

        ClassLoader cl = connectionContext.getClassLoader();
        if (cl == null) cl = getClass().getClassLoader();

        Class clazz = cl.loadClass(driverClass);

        driver = (Driver)clazz.newInstance();
        DriverManager.registerDriver(this.driver);

        String s = (String)properties.remove(INITIAL_SIZE);
        int initialSize = s == null ? 0 : Integer.parseInt(s);

        s = (String)properties.remove(MAX_ACTIVE);
        if (s != null) config.maxActive = Integer.parseInt(s);

        s = (String)properties.remove(MAX_IDLE);
        if (s != null) config.maxIdle = Integer.parseInt(s);

        s = (String)properties.remove(MAX_WAIT);
        if (s != null) config.maxWait = Integer.parseInt(s);

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

        //s = (String)properties.remove(WHEN_EXHAUSTED_ACTION);
        //if (s != null) config.whenExhaustedAction = Byte.parseByte(s);

        connectionPool = new GenericObjectPool(null, config);

        String validationQuery = (String)properties.remove(VALIDATION_QUERY);

        ConnectionFactory connectionFactory = new DriverConnectionFactory(this.driver, url, properties);

        //PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(
                        connectionFactory,
                        connectionPool,
                        null, // statement pool factory
                        validationQuery, // test query
                        false, // read only
                        true // auto commit
                );

        //log.debug("Initializing "+initialSize+" connections.");
        for (int i = 0; i < initialSize; i++) {
             connectionPool.addObject();
         }

        ds = new PoolingDataSource(connectionPool);

        client = new JDBCClient(driver, getParameters());
    }

    public void destroy() throws Exception {
        client.close();

        //connectionPool.close();
    }

    public JDBCClient getClient() throws Exception {
        return client;

        //return new JDBCClient(ds.getConnection(), getParameters());
    }

    public String getFieldNames(SourceConfig sourceConfig) throws Exception {
        StringBuilder sb = new StringBuilder();

        Collection<FieldConfig> fields = sourceConfig.getFieldConfigs();
        for (FieldConfig fieldConfig : fields) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(fieldConfig.getOriginalName());
        }

        return sb.toString();
    }

    public String getOringialPrimaryKeyFieldNamesAsString(SourceConfig sourceConfig) throws Exception {
        StringBuilder sb = new StringBuilder();

        Collection<String> fields = sourceConfig.getOriginalPrimaryKeyNames();
        for (String name : fields) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(name);
        }

        return sb.toString();
    }

    public JDBCClient getClient(Session session, Source source) throws Exception {

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
    }

    public void closeClient(Session session, Source source, JDBCClient client) throws Exception {

        //String authentication = source.getParameter(AUTHENTICATON);
        //if (debug) log.debug("Authentication: "+authentication);

        client.close();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            final Session session,
            final Source source,
            final AddRequest request,
            final AddResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session, source);

        try {
            InsertStatement statement = new InsertStatement();
            statement.setSource(source);

            RDN rdn = request.getDn().getRdn();

            if (rdn != null) {
                for (String name : rdn.getNames()) {

                    Object value = rdn.get(name);

                    Field field = source.getField(name);
                    if (field == null) throw new Exception("Unknown field: " + name);

                    statement.addAssignment(new Assignment(field, value));
                }
            }

            Attributes attributes = request.getAttributes();

            for (String name : attributes.getNames()) {
                if (rdn != null && rdn.contains(name)) continue;

                Object value = attributes.getValue(name); // get first value

                Field field = source.getField(name);
                if (field == null) throw new Exception("Unknown field: " + name);

                statement.addAssignment(new Assignment(field, value));
            }

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.setStatement(statement);

            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);

            log.debug("Add operation completed.");

        } finally {
            closeClient(session, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            final Session session,
            final Source source,
            final CompareRequest request,
            final CompareResponse response
    ) throws Exception {

        SearchRequest newRequest = new SearchRequest();
        newRequest.setDn(request.getDn());
        newRequest.setScope(SearchRequest.SCOPE_BASE);

        SimpleFilter filter = new SimpleFilter(request.getAttributeName(), "=", request.getAttributeValue());
        newRequest.setFilter(filter);

        SearchResponse newResponse = new SearchResponse();

        search(session, source, newRequest, newResponse);

        boolean result = newResponse.hasNext();

        if (debug) log.debug("Compare operation completed ["+result+"].");
        response.setReturnCode(result ? LDAP.COMPARE_TRUE : LDAP.COMPARE_FALSE);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            final Session session,
            final Source source,
            final DeleteRequest request,
            final DeleteResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session, source);

        try {
            DeleteStatement statement = new DeleteStatement();

            SourceRef sourceRef = new SourceRef(source);
            statement.setSourceRef(sourceRef);

            Filter filter = null;

            RDN rdn = request.getDn().getRdn();
            if (rdn != null) {
                for (String name : rdn.getNames()) {
                    Object value = rdn.get(name);

                    SimpleFilter sf = new SimpleFilter(name, "=", value);
                    filter = FilterTool.appendAndFilter(filter, sf);
                }
            }

            statement.setFilter(filter);

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.setStatement(statement);

            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);

            log.debug("Delete operation completed.");

        } finally {
            closeClient(session, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            final Session session,
            final Source source,
            final ModifyRequest request,
            final ModifyResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session, source);

        try {
            UpdateStatement statement = new UpdateStatement();

            SourceRef sourceRef = new SourceRef(source);
            statement.setSourceRef(sourceRef);

            RDN rdn = request.getDn().getRdn();

            Collection<Modification> modifications = request.getModifications();
            for (Modification modification : modifications) {

                int type = modification.getType();
                Attribute attribute = modification.getAttribute();
                String name = attribute.getName();

                Field field = source.getField(name);
                if (field == null) continue;

                switch (type) {
                    case Modification.ADD:
                    case Modification.REPLACE:
                        Object value = rdn.get(name);
                        if (value == null) value = attribute.getValue();
                        statement.addAssignment(new Assignment(field, value));
                        break;

                    case Modification.DELETE:
                        statement.addAssignment(new Assignment(field, null));
                        break;
                }
            }

            Filter filter = null;
            for (String name : rdn.getNames()) {
                Object value = rdn.get(name);

                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }

            statement.setFilter(filter);

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.setStatement(statement);

            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);

            log.debug("Modify operation completed.");

        } finally {
            closeClient(session, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            final Session session,
            final Source source,
            final ModRdnRequest request,
            final ModRdnResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session, source);

        try {
            UpdateStatement statement = new UpdateStatement();

            SourceRef sourceRef = new SourceRef(source);
            statement.setSourceRef(sourceRef);

            RDN newRdn = request.getNewRdn();
            for (String name : newRdn.getNames()) {
                Object value = newRdn.get(name);

                Field field = source.getField(name);
                if (field == null) continue;

                statement.addAssignment(new Assignment(field, value));
            }

            RDN rdn = request.getDn().getRdn();
            Filter filter = null;
            for (String name : rdn.getNames()) {
                Object value = rdn.get(name);

                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }

            statement.setFilter(filter);

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.setStatement(statement);

            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);

            log.debug("ModRdn operation completed.");

        } finally {
            closeClient(session, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final Source source,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session, source);

        try {
            response.setSizeLimit(request.getSizeLimit());

            SelectStatement statement = new SelectStatement();

            SourceRef sourceRef = new SourceRef(source);

            Filter filter = null;

            DN dn = request.getDn();
            if (dn != null) {
                RDN rdn = dn.getRdn();
                for (String name : rdn.getNames()) {
                    Object value = rdn.get(name);

                    SimpleFilter sf = new SimpleFilter(name, "=", value);
                    filter = FilterTool.appendAndFilter(filter, sf);
                }
            }

            filter = FilterTool.appendAndFilter(filter, request.getFilter());

            statement.addFieldRefs(sourceRef.getFieldRefs());
            statement.addSourceRef(sourceRef);
            statement.setFilter(filter);

            String where = source.getParameter(FILTER);
            if (where != null) {
                statement.setWhere(where);
            }

            statement.setOrders(sourceRef.getPrimaryKeyFieldRefs());

            QueryRequest queryRequest = new QueryRequest();
            queryRequest.setStatement(statement);

            QueryResponse queryResponse = new QueryResponse() {
                public void add(Object object) throws Exception {
                    ResultSet rs = (ResultSet)object;

                    if (sizeLimit > 0 && totalCount >= sizeLimit) {
                        throw LDAP.createException(LDAP.SIZE_LIMIT_EXCEEDED);
                    }

                    SearchResult searchResult = createSearchResult(source, rs);
                    response.add(searchResult);

                    totalCount++;
                }
                public void close() throws Exception {
                    response.close();
                    super.close();
                }
            };

            String sizeLimit = source.getParameter(SIZE_LIMIT);
            if (sizeLimit != null) {
                queryResponse.setSizeLimit(Long.parseLong(sizeLimit));
            }

            client.executeQuery(queryRequest, queryResponse);

            log.debug("Search operation completed.");

        } finally {
            closeClient(session, source, client);
        }
    }

    public SearchResult createSearchResult(
            Source source,
            ResultSet rs
    ) throws Exception {

        Attributes attributes = new Attributes();
        RDNBuilder rb = new RDNBuilder();

        int column = 1;

        for (Field field : source.getFields()) {

            Object value = rs.getObject(column++);
            if (value == null) continue;

            String fieldName = field.getName();
            attributes.addValue(fieldName, value);

            if (field.isPrimaryKey()) rb.set(fieldName, value);
        }

        DNBuilder db = new DNBuilder();
        db.append(rb.toRdn());

        String baseDn = source.getParameter(BASE_DN);
        if (baseDn != null) {
            db.append(baseDn);
        }

        DN dn = db.toDn();

        return new SearchResult(dn, attributes);
    }

    public Collection<String> getCatalogs() throws Exception {
        JDBCClient client = getClient();
        Collection<String> list = client.getCatalogs();
        client.close();
        return list;
    }
    
    public Collection<String> getSchemas() throws Exception {
        JDBCClient client = getClient();
        Collection<String> list = client.getSchemas();
        client.close();
        return list;
    }
    
    public Collection<TableConfig> getTables(String catalog, String schema) throws Exception {
        JDBCClient client = getClient();
        Collection<TableConfig> tables = client.getTables(catalog, schema);
        client.close();
        return tables;
    }
    
    public String getTableName(Source source) throws Exception {
        JDBCClient client = getClient();
        String tableName = client.getTableName(source.getSourceConfig());
        client.close();
        return tableName;
    }

    public String getTableName(SourceConfig sourceConfig) throws Exception {
        JDBCClient client = getClient();
        String tableName = client.getTableName(sourceConfig);
        client.close();
        return tableName;
    }

    public void createTable(SourceConfig sourceConfig) throws Exception {
        JDBCClient client = getClient();
        client.createTable(sourceConfig);
        client.close();
    }

    public boolean checkDatabase(String database) throws Exception {
        JDBCClient client = getClient();
        boolean b = client.checkDatabase(database);
        client.close();
        return b;
    }

    public void createDatabase(String database) throws Exception {
        JDBCClient client = getClient();
        client.createDatabase(database);
        client.close();
    }
    
    public void dropDatabase(String database) throws Exception {
        JDBCClient client = getClient();
        client.dropDatabase(database);
        client.close();
    }
    
    public void executeQuery(String sql, QueryResponse response) throws Exception {
        JDBCClient client = getClient();
        client.executeQuery(sql, response);
        client.close();
    }
    
    public void executeQuery(
            String sql,
            Collection<Assignment> parameters,
            QueryResponse response
    ) throws Exception {
        JDBCClient client = getClient();
        client.executeQuery(sql, parameters, response);
        client.close();
    }

    public int executeUpdate(
            String sql,
            Collection<Assignment> assignments
    ) throws Exception {
        JDBCClient client = getClient();
        int count = client.executeUpdate(sql, assignments);
        client.close();
        return count;
    }
}
