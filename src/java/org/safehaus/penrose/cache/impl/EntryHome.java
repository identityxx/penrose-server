/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.AttributeDefinition;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryHome {

    protected Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    public final static String MODIFY_TIME_FIELD = "__modifyTime";

    private DataSource ds;
    private DefaultCache cache;
    public EntryDefinition entry;
    public String tableName;

    public EntryHome(DataSource ds, DefaultCache cache, EntryDefinition entry, String tableName) throws Exception {

        this.ds = ds;
        this.cache = cache;
        this.entry = entry;
        this.tableName = tableName;

        try {
            drop();
        } catch (Exception e) {
            // ignore
        }

        create();
    }

    public void create() throws Exception {
        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();

            Collection rdnAttributes = entry.getRdnAttributes();

            for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
                AttributeDefinition attribute = (AttributeDefinition) i.next();
                String name = attribute.getName();

                if (sb.length() > 0) sb.append(", ");

                sb.append(name);
                sb.append(" varchar(255)");
            }

            sb.append(", "+MODIFY_TIME_FIELD+" datetime");

            String sql = "create table " + tableName + " (" + sb + ")";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public void drop() throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            String sql = "drop table " + tableName;
            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public String getPkAttributeNames() {
        StringBuffer sb = new StringBuffer();

        Collection rdnAttributes = entry.getRdnAttributes();

        for (Iterator j = rdnAttributes.iterator(); j.hasNext();) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            String name = attribute.getName();

            if (sb.length() > 0) sb.append(", ");
            sb.append(name);
        }

        return sb.toString();
    }

    public Row getRdn(ResultSet rs) throws Exception {
        Row values = new Row();

        Collection rdnAttributes = entry.getRdnAttributes();
        int c = 0;
        for (Iterator j = rdnAttributes.iterator(); j.hasNext();) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            String name = attribute.getName();

            Object value = rs.getObject(++c);
            if (value != null) values.set(name, value);
        }

        return values;
    }

    public Collection search(Row pk) throws Exception {
        List pks = new ArrayList();
        pks.add(pk);

        return search(pks);
    }

    public Collection search(Date date) throws Exception {

        String attributeNames = getPkAttributeNames();

        String sql = "select " + attributeNames + " from " + tableName
                + " where " + MODIFY_TIME_FIELD + " <= ?";

        sql += " order by "+attributeNames;

        List results = new ArrayList();

        log.debug("Executing " + sql);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ds.getConnection();
            ps = con.prepareStatement(sql);

            ps.setTimestamp(1, new Timestamp(date.getTime()));
            log.debug(" - 1 = "+date);

            rs = ps.executeQuery();

            while (rs.next()) {
                Row rdn = getRdn(rs);
                results.add(rdn);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }

        return results;
    }

    public Collection search(Collection rdns) throws Exception {

        if (rdns == null || rdns.isEmpty()) return new ArrayList();

        String attributeNames = getPkAttributeNames();

        Filter filter = cache.getCacheContext().getFilterTool().createFilter(rdns);
        String sqlFilter = cache.getCacheFilterTool().toSQLFilter(entry, filter, false);

        String sql = "select " + attributeNames + " from " + tableName
                + " where " + sqlFilter;

        sql += " order by "+attributeNames;

        List results = new ArrayList();

        log.debug("Executing " + sql);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ds.getConnection();
            ps = con.prepareStatement(sql);

            int counter = 0;
            for (Iterator i=rdns.iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();

                for (Iterator j=pk.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Object value = pk.get(name);

                    ps.setObject(++counter, value);
                    log.debug(" - "+counter+" = "+value);
                }
            }

            rs = ps.executeQuery();

            while (rs.next()) {
                Row rdn = getRdn(rs);
                results.add(rdn);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }

        return results;
    }

    public void insert(Row rdn, Date date) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();
            List parameters = new ArrayList();

            Collection rdnAttributes = entry.getRdnAttributes();

            for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
                AttributeDefinition attribute = (AttributeDefinition) i.next();
                String name = attribute.getName();
                Object value = rdn.get(name);

                if (value instanceof byte[]) {
                    value = new String((byte[]) value);
                }

                if (sb.length() > 0) {
                    sb.append(", ");
                    sb2.append(", ");
                }

                sb.append(name);
                sb2.append("?");
                parameters.add(value);
            }

            sb.append(", ");
            sb.append(MODIFY_TIME_FIELD);
            sb2.append(", ?");

            String sql = "insert into " + tableName + " (" + sb + ") values (" + sb2 + ")";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            int index = 0;
            for (Iterator i = parameters.iterator(); i.hasNext(); ) {
                Object parameter = i.next();
                ps.setObject(++index, parameter);
                log.debug("- " + index + " = " + parameter);
            }

            ps.setTimestamp(++index, new Timestamp(date.getTime()));

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public void delete(Row rdn) throws Exception {
        Collection rdns = new HashSet();
        rdns.add(rdn);
        delete(rdns);
    }

    public void delete(Collection rdns) throws Exception {

        if (rdns.size() == 0) return;

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();
            List parameters = new ArrayList();

            Collection rdnAttributes = entry.getRdnAttributes();

            for (Iterator i = rdns.iterator(); i.hasNext(); ) {
                Row rdn = (Row)i.next();

                if (sb.length() > 0) sb.append(" or ");

                StringBuffer sb2 = new StringBuffer();
                for (Iterator j = rdnAttributes.iterator(); j.hasNext();) {
                    AttributeDefinition attribute = (AttributeDefinition)j.next();
                    String name = attribute.getName();

                    if (sb2.length() > 0) sb2.append(" and ");
                    sb2.append(name + "=?");

                    Object value = rdn.get(name);

                    if (value instanceof byte[]) {
                        value = new String((byte[]) value);
                    }

                    parameters.add(value);
                }

                sb.append("(");
                sb.append(sb2);
                sb.append(")");
            }

            String sql = "delete from " + tableName + " where " + sb;

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            int index = 0;

            for (Iterator i = parameters.iterator(); i.hasNext();) {
                Object parameter = i.next();
                ps.setObject(++index, parameter);
                log.debug("- " + index + " = " + parameter);
            }

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
}
