/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
package org.safehaus.penrose.connection;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.*;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCAdapter extends Adapter {

    public final static String DRIVER     = "driver";
    public final static String URL        = "url";
    public final static String USER       = "user";
    public final static String PASSWORD   = "password";

    public final static String TABLE_NAME = "tableName";
    public final static String FILTER     = "filter";

    public DataSource ds;

    public JDBCFilterTool filterTool;

    public void init() throws Exception {
        String name = getConnectionName();

    	log.debug("-------------------------------------------------------------------------------");
    	log.debug("Initializing JDBC connection "+name+":");

        String driver = getParameter(DRIVER);
        String url = getParameter(URL);

        Class.forName(driver);

        Properties properties = new Properties();
        for (Iterator i=getParameterNames().iterator(); i.hasNext(); ) {
            String param = (String)i.next();
            String value = getParameter(param);
            log.debug(param+": "+value);
            properties.setProperty(param, value);
        }

        GenericObjectPool connectionPool = new GenericObjectPool(null);
        //connectionPool.setMaxActive(0);

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, properties);

        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(
                        connectionFactory,
                        connectionPool,
                        null, // statement pool factory
                        null, // test query
                        false, // read only
                        true // auto commit
                );

        ds = new PoolingDataSource(connectionPool);

        filterTool = new JDBCFilterTool(getAdapterContext());
    }

    public String getFieldNames(Source source) throws Exception {
        StringBuffer sb = new StringBuffer();

        Config config = getAdapterContext().getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        Collection fields = source.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(field.getName());

            if (sb.length() > 0) sb.append(", ");
            sb.append(fieldDefinition.getOriginalName());
        }

        return sb.toString();
    }

    public String getPkFieldNames(Source source) throws Exception {
        StringBuffer sb = new StringBuffer();

        Config config = getAdapterContext().getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        Collection fields = source.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(field.getName());
            if (!fieldDefinition.isPrimaryKey()) continue;

            if (sb.length() > 0) sb.append(", ");
            sb.append(fieldDefinition.getOriginalName());
        }

        return sb.toString();
    }

    public SearchResults search(Source source, Filter filter, long sizeLimit) throws Exception {
        SearchResults results = new SearchResults();

        //log.debug("--------------------------------------------------------------------------------------");
        log.debug("Searching JDBC Source: "+source.getConnectionName()+"/"+source.getSourceName()+": "+filter);

        Config config = getAdapterContext().getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        String tableName = sourceDefinition.getParameter(TABLE_NAME);
        String sqlFilter = sourceDefinition.getParameter(FILTER);
        log.debug("tableName: "+tableName);
        log.debug("filter: "+sqlFilter);

        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append(getPkFieldNames(source));
        sb.append(" from ");
        sb.append(tableName);

        StringBuffer sb2 = new StringBuffer();
        if (sqlFilter != null) {
            sb2.append(sqlFilter);
        }

        List parameters = new ArrayList();
        if (filter != null) {
            if (sb2.length() > 0) sb2.append(" and ");
            sb2.append(filterTool.convert(source, filter, parameters));
        }

        if (sb2.length() > 0) {
            sb.append(" where ");
            sb.append(sb2);
        }

        sb.append(" order by ");
        sb.append(getPkFieldNames(source));

        String sql = sb.toString();

        java.sql.Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = ds.getConnection();

            log.debug("Executing "+sql);

            ps = con.prepareStatement(sql);

            int counter = 0;
            for (Iterator i=parameters.iterator(); i.hasNext(); ) {
                Object param = i.next();
                ps.setObject(++counter, param);
                log.debug(" - "+counter+" = "+param);
            }

            rs = ps.executeQuery();

            log.debug("Result:");

            for (int i=0; rs.next() && (sizeLimit == 0 || i<sizeLimit); i++) {

                Row row = getPkValues(source, rs);
                log.debug(" - "+row);

                results.add(row);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        results.close();

        return results;
    }

    public SearchResults load(Source source, Filter filter, long sizeLimit) throws Exception {
        SearchResults results = new SearchResults();

        //log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading JDBC Source: "+source.getConnectionName()+"/"+source.getSourceName()+": "+filter);

        Config config = getAdapterContext().getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        String tableName = sourceDefinition.getParameter(TABLE_NAME);
        String sqlFilter = sourceDefinition.getParameter(FILTER);
        log.debug("tableName: "+tableName);
        log.debug("filter: "+sqlFilter);

        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append(getFieldNames(source));
        sb.append(" from ");
        sb.append(tableName);

        StringBuffer sb2 = new StringBuffer();
        if (sqlFilter != null) {
            sb2.append(sqlFilter);
        }

        List parameters = new ArrayList();
        if (filter != null) {
            if (sb2.length() > 0) sb2.append(" and ");
            sb2.append(filterTool.convert(source, filter, parameters));
        }

        if (sb2.length() > 0) {
            sb.append(" where ");
            sb.append(sb2);
        }

        sb.append(" order by ");
        sb.append(getPkFieldNames(source));

        String sql = sb.toString();

        java.sql.Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = ds.getConnection();

            log.debug("Executing "+sql);

            ps = con.prepareStatement(sql);

            int counter = 0;
            for (Iterator i=parameters.iterator(); i.hasNext(); ) {
                Object param = i.next();
                ps.setObject(++counter, param);
                log.debug(" - "+counter+" = "+param);
            }

            rs = ps.executeQuery();

            log.debug("Result:");

            for (int i=0; rs.next() && (sizeLimit == 0 || i<sizeLimit); i++) {

                AttributeValues av = getValues(source, rs);
                log.debug(" - "+av);

                results.add(av);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        results.close();

        return results;
    }

    public Row getPkValues(Source source, ResultSet rs) throws Exception {

        Row row = new Row();

        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();

        int c = 1;

        Config config = getAdapterContext().getConfig(source);
        Collection fields = config.getPrimaryKeyFields(source);

        for (Iterator i=fields.iterator(); i.hasNext() && c<=count; c++) {
            Field field = (Field)i.next();

            Object value = rs.getObject(c);
            if (value == null) continue;

            row.set(field.getName(), value);
        }

        //log.debug("=> values: "+row);

        return row;
    }

    public AttributeValues getValues(Source source, ResultSet rs) throws Exception {

        AttributeValues row = new AttributeValues();

        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();

        int c = 1;
        Collection fields = source.getFields();

        for (Iterator i=fields.iterator(); i.hasNext() && c<=count; c++) {
            Field field = (Field)i.next();
            
            Object value = rs.getObject(c);
            if (value == null) continue;

            row.add(field.getName(), value);
        }

        //log.debug("=> values: "+row);

        return row;
    }

    public int bind(Source source, AttributeValues values, String cred) throws Exception {
        return LDAPException.INVALID_CREDENTIALS;
    }

    public int add(Source source, AttributeValues fieldValues) throws Exception {

        // convert sets into single values
        Collection rows = getAdapterContext().getTransformEngine().convert(fieldValues);
    	Row row = (Row)rows.iterator().next();

        Config config = getAdapterContext().getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        String tableName = sourceDefinition.getParameter(TABLE_NAME);

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();

            Collection fields = source.getFields();
            Collection parameters = new ArrayList();
            for (Iterator i=fields.iterator(); i.hasNext(); ) {
                Field field = (Field)i.next();
                FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(field.getName());

                if (sb.length() > 0) {
                    sb.append(", ");
                    sb2.append(", ");
                }

                sb.append(fieldDefinition.getOriginalName());
                sb2.append("?");

                Object obj = row.get(field.getName());
                parameters.add(obj);
            }

            String sql = "insert into "+tableName+" ("+sb+") values ("+sb2+")";
            log.debug("Executing "+sql);

            ps = con.prepareStatement(sql);

            int c = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); c++) {
                Object obj = i.next();
                ps.setObject(c, obj);
                log.debug(" - "+c+" = "+(obj == null ? null : obj.toString()));
            }

            ps.executeUpdate();

        } catch (Exception e) {
            log.debug("Add failed: ("+e.getClass().getName()+") "+e.getMessage());
            return LDAPException.ENTRY_ALREADY_EXISTS;

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public Map getPkValues(Source source, Map entry) throws Exception {

        Map pk = new HashMap();

        Config config = getAdapterContext().getConfig(source);
        Collection fields = config.getPrimaryKeyFields(source);

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            String name = field.getName();

            Object value = entry.get(name);
            if (value == null) continue;

            pk.put(name, value);
        }

        return pk;
    }

    public int delete(Source source, AttributeValues fieldValues) throws Exception {
        Map pk = getPkValues(source, fieldValues.getValues());
        //log.debug("Deleting entry "+pk);

        // convert sets into single values
        Collection pkRows = getAdapterContext().getTransformEngine().convert(pk);
        //Collection rows = getAdapterContext().getTransformEngine().convert(fieldValues);

        Row pkRow = (Row)pkRows.iterator().next();
        //Row row = (Row)rows.iterator().next();

        Config config = getAdapterContext().getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        String tableName = sourceDefinition.getParameter(TABLE_NAME);

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            StringBuffer sb = new StringBuffer();
            for (Iterator i=pkRow.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();

                if (sb.length() > 0) sb.append(" and ");

                sb.append(name);
                sb.append("=?");
            }

            String sql = "delete from "+tableName+" where "+sb;
            log.debug("Executing "+sql);

            ps = con.prepareStatement(sql);

            int c = 1;
            for (Iterator i=pkRow.getNames().iterator(); i.hasNext(); c++) {
                String name = (String)i.next();
                Object value = pkRow.get(name);
                ps.setObject(c, value);
                log.debug(" - "+c+" = "+value);
            }

            int count = ps.executeUpdate();
            if (count == 0) return LDAPException.NO_SUCH_OBJECT;

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int modify(Source source, AttributeValues oldEntry, AttributeValues newEntry) throws Exception {

        // convert sets into single values
        Collection oldRows = getAdapterContext().getTransformEngine().convert(oldEntry);
        Collection newRows = getAdapterContext().getTransformEngine().convert(newEntry);

        Row oldRow = (Row)oldRows.iterator().next();
        Row newRow = (Row)newRows.iterator().next();

        //log.debug("Modifying source "+source.getName()+": "+oldRow+" with "+newRow);

        Config config = getAdapterContext().getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        String tableName = sourceDefinition.getParameter(TABLE_NAME);

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();
            Collection parameters = new ArrayList();

            Collection fields = source.getFields();
            for (Iterator i=fields.iterator(); i.hasNext(); ) {
                Field field = (Field)i.next();
                FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(field.getName());

                if (sb.length() > 0) sb.append(", ");

                sb.append(fieldDefinition.getOriginalName());
                sb.append("=?");

                Object value = newRow.get(field.getName());
                parameters.add(value);
            }

            for (Iterator i=fields.iterator(); i.hasNext(); ) {
                Field field = (Field)i.next();
                FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(field.getName());
                if (!fieldDefinition.isPrimaryKey()) continue;

                if (sb2.length() > 0) sb2.append(" and ");

                sb2.append(fieldDefinition.getOriginalName());
                sb2.append("=?");

                Object value = oldRow.get(field.getName());
                parameters.add(value);
            }

            String sql = "update "+tableName+" set "+sb+" where "+sb2;
            log.debug("Executing "+sql);

            ps = con.prepareStatement(sql);

            int c = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); c++) {
                Object value = i.next();
                ps.setObject(c, value);
                log.debug(" - "+c+" = "+value);
            }

            int count = ps.executeUpdate();
            if (count == 0) return LDAPException.NO_SUCH_OBJECT;

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
        
        return LDAPException.SUCCESS;
    }
    
}
