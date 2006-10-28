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
import java.lang.reflect.Field;

import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.jdbc.JDBCAdapter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class JDBCClient {

    Logger log = LoggerFactory.getLogger(getClass());

    String driver;
    String url;
    String username;
    String password;

    DatabaseMetaData dmd;

    Connection connection;

    public JDBCClient(Map parameters) throws Exception {
        this.driver = (String)parameters.get(JDBCAdapter.DRIVER);
        this.url = (String)parameters.get(JDBCAdapter.URL);
        this.username = (String)parameters.get(JDBCAdapter.USER);
        this.password = (String)parameters.get(JDBCAdapter.PASSWORD);
    }

    public JDBCClient(
            String driver,
            String url,
            String username,
            String password) throws Exception {

        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public void connect() throws Exception {
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        dmd = connection.getMetaData();
    }

    public void close() throws Exception{
        connection.close();
    }

    public String getTypeName(int type) throws Exception {
        Field fields[] = Types.class.getFields();
        for (int i=0; i<fields.length; i++) {
            Field field = fields[i];
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

        ResultSet rs = dmd.getColumns(catalog, schema, tableName, "%");

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

        rs.close();

        rs = dmd.getPrimaryKeys(catalog, schema, tableName);

        while (rs.next()) {
            String name = rs.getString(4);

            FieldConfig field = (FieldConfig)columns.get(name);
            field.setPrimaryKey(FieldConfig.PRIMARY_KEY_TRUE);
        }

        rs.close();

        return columns.values();
    }

    public Collection getCatalogs() throws Exception {
        log.debug("Getting catalogs");
        ResultSet rs = dmd.getCatalogs();

        Collection catalogs = new ArrayList();
        while (rs.next()) {
            String catalogName = rs.getString(1);
            log.debug(" - "+catalogName);
            catalogs.add(catalogName);
        }

        rs.close();

        return catalogs;
    }

    public Collection getSchemas() throws Exception {
        log.debug("Getting schemas");
        ResultSet rs = dmd.getSchemas();

        Collection schemas = new ArrayList();
        while (rs.next()) {
            String schemaName = rs.getString(1);
            log.debug(" - "+schemaName);
            schemas.add(schemaName);
        }

        rs.close();

        return schemas;
    }

    public Collection getTables() throws SQLException {
        return getTables(null, null);
    }

    public Collection getTables(String catalog, String schema) throws SQLException {

        log.debug("Getting table names for "+catalog+" "+schema);
        Collection tables = new TreeSet();

        /*
           * String[] tableTypes = { "TABLE", "VIEW", "ALIAS", "SYNONYM", "GLOBAL
           * TEMPORARY", "LOCAL TEMPORARY", "SYSTEM TABLE" };
           */
        String[] tableTypes = { "TABLE", "VIEW", "ALIAS", "SYNONYM" };
        ResultSet rs = dmd.getTables(catalog, schema, "%", tableTypes);

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

        rs.close();

        return tables;
    }
}