/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.connection;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.*;
import org.hibernate.dialect.Dialect;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCAdapter extends Adapter {

    public final static String DRIVER   = "driver";
    public final static String URL      = "url";
    public final static String USER     = "user";
    public final static String PASSWORD = "password";
    public final static String DIALECT = "dialect";

    public DataSource ds;
    public Dialect dialect;

    public JDBCFilterTool filterTool;

    public void init() throws Exception {
        String name = getConnectionName();

    	log.debug("-------------------------------------------------------------------------------");
    	log.debug("Initializing JDBC connection "+name+":");

        String driver = getParameter(DRIVER);
        String url = getParameter(URL);
        String username = getParameter(USER);
        String password = getParameter(PASSWORD);

        String dialectClass = getParameter(DIALECT);
        dialect = (Dialect)Class.forName(dialectClass).newInstance();
        String lowerCaseFunction = dialect.getLowercaseFunction();
        log.debug("Lower case function: "+lowerCaseFunction);


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

        filterTool = new JDBCFilterTool();
    }

    public String getFieldNames(Source source) {
        StringBuffer sb = new StringBuffer();

        Collection fields = source.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();

            if (sb.length() > 0) sb.append(", ");
            //sb.append(source.getName());
            //sb.append(".");
            sb.append(field.getName());
        }

        return sb.toString();
    }

    public SearchResults search(Source source, Filter filter, long sizeLimit) throws Exception {
        SearchResults results = new SearchResults();

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("JDBC Source: "+source.getConnectionName());

        String tableName = source.getParameter("tableName");

        String fieldNames = getFieldNames(source);
        String sql = "select "+fieldNames+" from "+tableName; //+" "+source.getName();

        if (filter != null) {
            sql += " where "+filterTool.convert(source, filter);
        }

        java.sql.Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = ds.getConnection();

            log.debug("Executing "+sql);

            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();

            log.debug("Result:");

            for (int i=0; rs.next() && (sizeLimit == 0 || i<sizeLimit); i++) {

                Row row = getRow(source, rs);
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

    public Row getRow(Source source, ResultSet rs) throws Exception {

        Row row = new Row();

        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();

        int c = 1;
        Collection fields = source.getFields();

        for (Iterator i=fields.iterator(); i.hasNext() && c<=count; c++) {
            Field field = (Field)i.next();
            
            Object value = rs.getObject(c);
            if (value == null) continue;

            row.set(field.getName(), value);
        }

        //log.debug("=> values: "+row);

        return row;
    }

    public int bind(Source source, AttributeValues values, String cred) throws Exception {
        return LDAPException.INVALID_CREDENTIALS;
    }

    public int add(Source source, AttributeValues entry) throws Exception {

        String tableName = source.getParameter("tableName");

        // convert sets into single values
        Collection rows = getAdapterContext().getTransformEngine().convert(entry);
    	Row row = (Row)rows.iterator().next();

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();

            for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();

                if (sb.length() > 0) {
                    sb.append(", ");
                    sb2.append(", ");
                }

                sb.append(name);
                sb2.append("?");
            }

            String sql = "insert into "+tableName+" ("+sb+") values ("+sb2+")";
            log.debug("Executing "+sql);

            ps = con.prepareStatement(sql);

            int c = 1;
            for (Iterator i=row.getNames().iterator(); i.hasNext(); c++) {
                String name = (String)i.next();
                Object obj = row.get(name);
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

        Collection fields = source.getPrimaryKeyFields();

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();

            String name = field.getName();
            pk.put(name, entry.get(name));
        }

        return pk;
    }

    public int delete(Source source, AttributeValues entry) throws Exception {
        Map pk = getPkValues(source, entry.getValues());
        log.debug("Deleting entry "+pk);

        String tableName = source.getParameter("tableName");

        // convert sets into single values
        Collection pkRows = getAdapterContext().getTransformEngine().convert(pk);
        Collection rows = getAdapterContext().getTransformEngine().convert(entry);

    	Row pkRow = (Row)pkRows.iterator().next();
    	Row row = (Row)rows.iterator().next();

    	log.debug("Deleting attributes in "+pk);

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
                Object value = row.get(name);
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
        Map pk = getPkValues(source, newEntry.getValues());

        String tableName = source.getParameter("tableName");

        // convert sets into single values
        Collection oldRows = getAdapterContext().getTransformEngine().convert(oldEntry);
        Collection newRows = getAdapterContext().getTransformEngine().convert(newEntry);

        Row oldRow = (Row)oldRows.iterator().next();
    	Row newRow = (Row)newRows.iterator().next();
        log.debug("Replacing "+oldRow+" with "+newRow);

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            StringBuffer sb = new StringBuffer();
            for (Iterator i=newRow.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();

                if (sb.length() > 0) sb.append(", ");

                sb.append(name);
                sb.append("=?");
            }

            StringBuffer sb2 = new StringBuffer();
            for (Iterator i=pk.keySet().iterator(); i.hasNext(); ) {
                String name = (String)i.next();

                if (sb2.length() > 0) sb2.append(" and ");

                sb2.append(name);
                sb2.append("=?");
            }

            String sql = "update "+tableName+" set "+sb+" where "+sb2;
            log.debug("Executing "+sql);

            ps = con.prepareStatement(sql);

            int c = 1;
            for (Iterator i=newRow.getNames().iterator(); i.hasNext(); c++) {
                String name = (String)i.next();
                Object value = newRow.get(name);
                ps.setObject(c, value);
                log.debug(" - "+c+" = "+value);
            }

            for (Iterator i=pk.keySet().iterator(); i.hasNext(); c++) {
                String name = (String)i.next();
                Object value = oldRow.get(name);
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
