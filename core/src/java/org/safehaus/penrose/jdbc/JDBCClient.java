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

import org.safehaus.penrose.source.*;
import org.safehaus.penrose.util.TextUtil;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class JDBCClient {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static String DRIVER       = "driver";
    public final static String URL          = "url";
    public final static String USER         = "user";
    public final static String PASSWORD     = "password";

    public JDBCConnectionFactory connectionFactory;
    public Connection connection;

    public Integer queryTimeout;
    public String quote;

    public JDBCClient(Map<String,String> parameters) throws Exception {
        this(new JDBCConnectionFactory(parameters));
    }

    public JDBCClient(JDBCConnectionFactory connectionFactory) throws Exception {
        this.connectionFactory = connectionFactory;

        queryTimeout = connectionFactory.getQueryTimeout();
        quote = connectionFactory.getQuote();
    }

    public Connection getConnection() throws Exception {
        connect();
        return connection;
    }


    public synchronized void connect() throws Exception {

        if (connection == null || connection.isClosed()) {
            log.debug("Creating new JDBC connection.");
            connection = connectionFactory.createConnection();
        }
    }

    public synchronized void close() throws Exception {
        if (connection != null) connection.close();
        connection = null;
    }

    public String getTypeName(int type) throws Exception {
        java.lang.reflect.Field fields[] = Types.class.getFields();
        for (java.lang.reflect.Field field : fields) {
            if (field.getInt(null) != type) continue;
            return field.getName();
        }
        return "UNKNOWN";
    }

    public Collection<FieldConfig> getColumns(String tableName) throws Exception {
        return getColumns(null, null, tableName);
    }

    public Collection<FieldConfig> getColumns(String catalog, String schema, String tableName) throws Exception {

        log.debug("Getting column names for "+tableName+" "+catalog+" "+schema);

        Map<String,FieldConfig> columns = new HashMap<String,FieldConfig>();

        Connection connection = getConnection();
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

                FieldConfig field = columns.get(name);
                field.setPrimaryKey(true);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        Collection<FieldConfig> results = new ArrayList<FieldConfig>();
        results.addAll(columns.values());
        return results;
    }

    public Collection<String> getCatalogs() throws Exception {

        log.debug("Getting catalogs");

        Collection<String> catalogs = new ArrayList<String>();

        Connection connection = getConnection();
        ResultSet rs = null;

        try {
            DatabaseMetaData dmd = connection.getMetaData();

            rs = dmd.getCatalogs();

            while (rs.next()) {
                String catalogName = rs.getString(1);
                log.debug(" - "+catalogName);
                catalogs.add(catalogName);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return catalogs;
    }

    public Collection<String> getSchemas() throws Exception {

        log.debug("Getting schemas");

        Collection<String> schemas = new ArrayList<String>();

        Connection connection = getConnection();
        ResultSet rs = null;

        try {
            DatabaseMetaData dmd = connection.getMetaData();

            rs = dmd.getSchemas();

            while (rs.next()) {
                String schemaName = rs.getString(1);
                log.debug(" - "+schemaName);
                schemas.add(schemaName);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return schemas;
    }

    public Collection<Table> getTables() throws Exception {
        return getTables(null, null);
    }

    public Collection<Table> getTables(String catalog, String schema) throws Exception {

        log.debug("Getting table names for "+catalog+" "+schema);

        Collection<Table> tables = new ArrayList<Table>();

        Connection connection = getConnection();
        ResultSet rs = null;

        try {
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

                log.debug(" - "+tableCatalog+" "+tableSchema+" "+tableName);
                Table table = new Table(tableName, tableType, tableCatalog, tableSchema);
                tables.add(table);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return tables;
    }

    public int executeUpdate(
            String sql
    ) throws Exception {
        UpdateResponse response = new UpdateResponse();
        executeUpdate(sql, null, response);
        return response.getRowCount();
    }

    public int executeUpdate(
            String sql,
            Object[] parameters
    ) throws Exception {
        UpdateResponse response = new UpdateResponse();
        executeUpdate(sql, Arrays.asList(parameters), response);
        return response.getRowCount();
    }

    public int executeUpdate(
            String sql,
            Collection<Object> parameters
    ) throws Exception {
        UpdateResponse response = new UpdateResponse();
        executeUpdate(sql, parameters, response);
        return response.getRowCount();
    }

    public void executeUpdate(
            String sql,
            Collection<Object> parameters,
            UpdateResponse response
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            Collection<String> lines = TextUtil.split(sql, 80);
            for (String line : lines) {
                log.debug(TextUtil.displayLine(line, 80));
            }
            log.debug(TextUtil.displaySeparator(80));

            if (parameters != null && !parameters.isEmpty()) {
                log.debug(TextUtil.displayLine("Parameters:", 80));
                int counter = 1;
                for (Object value : parameters) {
                    log.debug(TextUtil.displayLine(" - "+counter+" = "+value, 80));
                    counter++;
                }
                log.debug(TextUtil.displaySeparator(80));
            }
        }

        Connection connection = getConnection();
        PreparedStatement ps = null;

        try {
            ps = connection.prepareStatement(sql);
            if (queryTimeout != null) ps.setQueryTimeout(queryTimeout);

            if (parameters != null && !parameters.isEmpty()) {
                int counter = 1;
                for (Object value : parameters) {
                    setParameter(ps, counter, value);
                    counter++;
                }
            }

            int count = ps.executeUpdate();
            response.setRowCount(count);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }
    }

    public void executeQuery(String sql, QueryResponse response) throws Exception {
        executeQuery(sql, (Collection<Object>)null, response);
    }

    public void executeQuery(
            String sql,
            Object[] parameters,
            QueryResponse response
    ) throws Exception {
        executeQuery(sql, Arrays.asList(parameters), response);
    }

    public void executeQuery(
            String sql,
            Collection<Object> parameters,
            QueryResponse response
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            Collection<String> lines = TextUtil.split(sql, 80);
            for (String line : lines) {
                log.debug(TextUtil.displayLine(line, 80));
            }
            log.debug(TextUtil.displaySeparator(80));

           if (parameters != null && !parameters.isEmpty()) {
                log.debug(TextUtil.displayLine("Parameters:", 80));
                int counter = 1;
                for (Object value : parameters) {

                    String v;
                    if (value instanceof byte[]) {
                        v = new String((byte[])value);
                    } else {
                        v = value.toString();
                    }

                    log.debug(TextUtil.displayLine(" - "+counter+" = "+v, 80));

                    counter++;
                }
                log.debug(TextUtil.displaySeparator(80));
           }
        }

        Connection connection = getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = connection.prepareStatement(sql);
            if (queryTimeout != null) ps.setQueryTimeout(queryTimeout);

            if (parameters != null && !parameters.isEmpty()) {
                int counter = 1;
                for (Object value : parameters) {
                    setParameter(ps, counter, value);
                    counter++;
                }
            }

            if (debug) log.debug("Executing query...");

            long t1 = System.currentTimeMillis();

            rs = ps.executeQuery();

            long t2 = System.currentTimeMillis();

            if (debug) log.debug("Execution completed. Elapsed time: "+(t2-t1)+" ms.");

            while (rs.next()) {
                if (response.isClosed()) return;
                response.add(rs);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (ps != null) try { ps.close(); } catch (Exception e) { log.error(e.getMessage(), e); }

            response.close();
        }
    }

    public void setParameter(PreparedStatement ps, int paramIndex, Object object) throws Exception {
    	ps.setObject(paramIndex, object);
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public String getQuote() {
        return quote;
    }
}