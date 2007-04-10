/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.jdbc;

import java.sql.*;
import java.util.*;

import javax.sql.DataSource;

import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.TableConfig;
import org.safehaus.penrose.adapter.jdbc.JDBCStatementBuilder;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.source.Source;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;

public class JDBCClient {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String DRIVER       = "driver";
    public final static String URL          = "url";
    public final static String USER         = "user";
    public final static String PASSWORD     = "password";

    public final static String CATALOG      = "catalog";
    public final static String SCHEMA       = "schema";
    public final static String TABLE        = "table";
    public final static String TABLE_NAME   = "tableName";
    public final static String FILTER       = "filter";

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

    public Properties properties = new Properties();

    public GenericObjectPool.Config config = new GenericObjectPool.Config();
    public GenericObjectPool connectionPool;
    public DataSource ds;

    public JDBCClient(Map properties) throws Exception {
        this.properties.putAll(properties);
    }

    public JDBCClient(
            String driver,
            String url,
            String username,
            String password
    ) throws Exception {

        properties.put(DRIVER, driver);
        properties.put(URL, url);
        properties.put(USER, username);
        properties.put(PASSWORD, password);
    }

    public void connect() throws Exception {

        String driver = (String)properties.remove(DRIVER);
        String url = (String)properties.remove(URL);

        Class.forName(driver);

        String s = (String)properties.remove(INITIAL_SIZE);
        int initialSize = s == null ? 1 : Integer.parseInt(s);

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
        if (s != null) config.testOnBorrow = new Boolean(s).booleanValue();

        s = (String)properties.remove(TEST_ON_RETURN);
        if (s != null) config.testOnReturn = new Boolean(s).booleanValue();

        s = (String)properties.remove(TEST_WHILE_IDLE);
        if (s != null) config.testWhileIdle = new Boolean(s).booleanValue();

        s = (String)properties.remove(TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        if (s != null) config.timeBetweenEvictionRunsMillis = Integer.parseInt(s);

        //s = (String)properties.remove(SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        //if (s != null) config.softMinEvictableIdleTimeMillis = Integer.parseInt(s);

        //s = (String)properties.remove(WHEN_EXHAUSTED_ACTION);
        //if (s != null) config.whenExhaustedAction = Byte.parseByte(s);

        connectionPool = new GenericObjectPool(null, config);

        String validationQuery = (String)properties.remove(VALIDATION_QUERY);

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, properties);

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
    }

    public Connection getConnection() throws Exception {
        return ds.getConnection();
    }

    public void close() throws Exception {
        connectionPool.close();
    }

    public String getTypeName(int type) throws Exception {
        java.lang.reflect.Field fields[] = Types.class.getFields();
        for (int i=0; i<fields.length; i++) {
            java.lang.reflect.Field field = fields[i];
            if (field.getInt(null) != type) continue;
            return field.getName();
        }
        return "UNKNOWN";
    }

    public Collection getColumns(String tableName) throws Exception {
        return getColumns(null, null, tableName);
    }

    public Collection getColumns(String catalog, String schema, String tableName) throws Exception {

        log.debug("Getting column names for "+tableName+" "+catalog+" "+schema);

        Map columns = new HashMap();

        Connection connection = null;

        try {
            connection = getConnection();
            DatabaseMetaData dmd = connection.getMetaData();

            ResultSet rs = null;

            try {
                rs = dmd.getColumns(catalog, schema, tableName, "%");

                while (rs.next()) {
                    //String tableCatalog = rs.getString(1);
                    //String tableSchema = rs.getString(2);
                    //String tableNm = rs.getString(3);
                    String columnName = rs.getString(4);
                    String columnType = getTypeName(rs.getInt(5));
                    int length = rs.getInt(7);
                    int precision = rs.getInt(9);

                    log.debug(" - "+columnName+" "+columnType+" ("+length+","+precision+")");

                    FieldConfig field = new FieldConfig(columnName);
                    field.setOriginalName(columnName);
                    field.setType(columnType);
                    field.setLength(length);
                    field.setPrecision(precision);

                    columns.put(columnName, field);
                }

            } finally {
                if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            }

            rs = null;
            try {
                rs = dmd.getPrimaryKeys(catalog, schema, tableName);

                while (rs.next()) {
                    String name = rs.getString(4);

                    FieldConfig field = (FieldConfig)columns.get(name);
                    field.setPrimaryKey(true);
                }

            } finally {
                if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            }

        } finally {
            if (connection != null) try { connection.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return columns.values();
    }

    public Collection getCatalogs() throws Exception {

        log.debug("Getting catalogs");

        Collection catalogs = new ArrayList();

        Connection connection = null;
        ResultSet rs = null;

        try {
            connection = getConnection();
            DatabaseMetaData dmd = connection.getMetaData();

            rs = dmd.getCatalogs();

            while (rs.next()) {
                String catalogName = rs.getString(1);
                log.debug(" - "+catalogName);
                catalogs.add(catalogName);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (connection != null) try { connection.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return catalogs;
    }

    public Collection getSchemas() throws Exception {

        log.debug("Getting schemas");

        Collection schemas = new ArrayList();

        Connection connection = null;
        ResultSet rs = null;

        try {
            connection = getConnection();
            DatabaseMetaData dmd = connection.getMetaData();

            rs = dmd.getSchemas();

            while (rs.next()) {
                String schemaName = rs.getString(1);
                log.debug(" - "+schemaName);
                schemas.add(schemaName);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (connection != null) try { connection.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return schemas;
    }

    public Collection getTables() throws Exception {
        return getTables(null, null);
    }

    public Collection getTables(String catalog, String schema) throws Exception {

        log.debug("Getting table names for "+catalog+" "+schema);

        Collection tables = new TreeSet();

        Connection connection = null;
        ResultSet rs = null;

        try {
            connection = getConnection();
            DatabaseMetaData dmd = connection.getMetaData();

            // String[] tableTypes = { "TABLE", "VIEW", "ALIAS", "SYNONYM", "GLOBAL
            // TEMPORARY", "LOCAL TEMPORARY", "SYSTEM TABLE" };
            String[] tableTypes = { "TABLE", "VIEW", "ALIAS", "SYNONYM" };
            rs = dmd.getTables(catalog, schema, "%", tableTypes);

            while (rs.next()) {
                String tableCatalog = rs.getString(1);
                String tableSchema = rs.getString(2);
                String tableName = rs.getString(3);
                String tableType = rs.getString(4);
                //String remarks = rs.getString(5);

                //log.debug(" - "+tableSchema+" "+tableName);
                TableConfig tableConfig = new TableConfig(tableName, tableType, tableCatalog, tableSchema);
                tables.add(tableConfig);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (connection != null) try { connection.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return tables;
    }

    public void executeUpdate(UpdateRequest request, UpdateResponse response) throws Exception {
        JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder();
        String sql = statementBuilder.generate(request.getStatement());
        Collection<Assignment> assignments = statementBuilder.getAssigments();
        executeUpdate(sql, assignments, response);
    }

    public void executeUpdate(String sql, UpdateResponse response) throws Exception {
        executeUpdate(sql, null, response);
    }

    public void executeUpdate(
            String sql,
            Collection<Assignment> assignments,
            UpdateResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            Collection lines = org.safehaus.penrose.util.Formatter.split(sql, 80);
            for (Iterator j=lines.iterator(); j.hasNext(); ) {
                String line = (String)j.next();
                log.debug(org.safehaus.penrose.util.Formatter.displayLine(line, 80));
            }
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));

            if (assignments != null && !assignments.isEmpty()) {
                log.debug(org.safehaus.penrose.util.Formatter.displayLine("Parameters:", 80));
                int counter = 1;
                for (Iterator j=assignments.iterator(); j.hasNext(); counter++) {
                    Assignment assignment = (Assignment)j.next();
                    Object value = assignment.getValue();
                    log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - "+counter+" = "+value, 80));
                }
                log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            }
        }

        Connection connection = null;
        PreparedStatement ps = null;

        try {
            connection = getConnection();
            ps = connection.prepareStatement(sql);

            if (assignments != null) {
                int counter = 1;
                for (Iterator j=assignments.iterator(); j.hasNext(); counter++) {
                    Assignment assignment = (Assignment)j.next();
                    setParameter(ps, counter, assignment);
                }
            }

            int count = ps.executeUpdate();
            response.setRowCount(count);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (connection != null) try { connection.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }
    }

    public void executeQuery(QueryRequest request, QueryResponse response) throws Exception {
        JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder();
        String sql = statementBuilder.generate(request.getStatement());
        Collection<Assignment> assignments = statementBuilder.getAssigments();
        executeQuery(sql, assignments, response);
    }

    public void executeQuery(String sql, QueryResponse response) throws Exception {
        executeQuery(sql, null, response);
    }

    public void executeQuery(
            String sql,
            Collection<Assignment> parameters,
            QueryResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

       if (debug) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            Collection lines = org.safehaus.penrose.util.Formatter.split(sql, 80);
            for (Iterator i=lines.iterator(); i.hasNext(); ) {
                String line = (String)i.next();
                log.debug(org.safehaus.penrose.util.Formatter.displayLine(line, 80));
            }
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));

           if (parameters != null && !parameters.isEmpty()) {
                log.debug(org.safehaus.penrose.util.Formatter.displayLine("Parameters:", 80));
                int counter = 1;
                for (Iterator j=parameters.iterator(); j.hasNext(); counter++) {
                    Assignment assignment = (Assignment)j.next();
                    Object value = assignment.getValue();
                    log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - "+counter+" = "+value, 80));
                }
                log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
           }
        }

        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            connection = getConnection();

            ps = connection.prepareStatement(sql);

            if (parameters != null) {
                int counter = 1;
                for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                    Assignment assignment = (Assignment)i.next();
                    setParameter(ps, counter, assignment);
                }
            }

            rs = ps.executeQuery();

            while (rs.next()) {
                response.add(rs);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (ps != null) try { ps.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (connection != null) try { connection.close(); } catch (Exception e) { log.error(e.getMessage(), e); }

            response.close();
        }
    }

    public void setParameter(PreparedStatement ps, int paramIndex, Assignment assignment) throws Exception {
    	ps.setObject(paramIndex, assignment.getValue());
    }

    public void createTable(Source source) throws Exception {

        String tableName = source.getParameter(JDBCClient.TABLE);

        StringBuilder sb = new StringBuilder();

        sb.append("create table ");
        sb.append(tableName);
        sb.append(" (");

        boolean first = true;
        for (Iterator i=source.getFields().iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(field.getName());
            sb.append(" ");
            sb.append(field.getType());

            if ("VARCHAR".equals(field.getType()) && field.getLength() > 0) {
                sb.append("(");
                sb.append(field.getLength());
                sb.append(")");
            }

            if (field.isCaseSensitive()) {
                sb.append(" binary");
            }
        }

        Collection<String> indexFieldNames = source.getIndexFieldNames();
        for (Iterator i=indexFieldNames.iterator(); i.hasNext(); ) {
            String fieldName = (String)i.next();

            sb.append(", index (");
            sb.append(fieldName);
            sb.append(")");
        }

        Collection<String> primaryKeyNames = source.getPrimaryKeyNames();
        if (!primaryKeyNames.isEmpty()) {
            sb.append(", primary key (");

            first = true;
            for (Iterator i=primaryKeyNames.iterator(); i.hasNext(); ) {
                String fieldName = (String)i.next();

                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }

                sb.append(fieldName);
            }

            sb.append(")");
        }

        sb.append(")");

        String sql = sb.toString();

        UpdateResponse response = new UpdateResponse();

        executeUpdate(sql, null, response);
    }

    public void renameTable(Source oldSource, Source newSource) throws Exception {

        String oldTableName = oldSource.getParameter(JDBCClient.TABLE);
        String newTableName = newSource.getParameter(JDBCClient.TABLE);

        StringBuilder sb = new StringBuilder();

        sb.append("rename table ");
        sb.append(oldTableName);
        sb.append(" to ");
        sb.append(newTableName);

        String sql = sb.toString();

        UpdateResponse response = new UpdateResponse();

        executeUpdate(sql, null, response);
    }

    public void dropTable(Source source) throws Exception {

        String tableName = source.getParameter(JDBCClient.TABLE);

        StringBuilder sb = new StringBuilder();

        sb.append("drop table ");
        sb.append(tableName);

        String sql = sb.toString();

        UpdateResponse response = new UpdateResponse();

        executeUpdate(sql, null, response);
    }

    public void cleanTable(Source source) throws Exception {

        String tableName = source.getParameter(JDBCClient.TABLE);

        StringBuilder sb = new StringBuilder();

        sb.append("delete from ");
        sb.append(tableName);

        String sql = sb.toString();

        UpdateResponse response = new UpdateResponse();

        executeUpdate(sql, null, response);
    }
}