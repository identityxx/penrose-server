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
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryAttributeHome {

    protected Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    public final static String MODIFY_TIME_FIELD = "__modifyTime";

    private DataSource ds;
    private DefaultCache cache;
    public EntryDefinition entry;
    public String tableName;

    public EntryAttributeHome(DataSource ds, DefaultCache cache, EntryDefinition entry, String tableName) throws Exception {

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

            Set set = new HashSet();
            Collection rdnAttributes = entry.getRdnAttributes();

            for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
                AttributeDefinition attributeDefinition = (AttributeDefinition) i.next();
                String name = attributeDefinition.getName();

                // TODO need to handle multiple attribute definitions
                if (set.contains(name)) continue;
                set.add(name);

                if (sb.length() > 0) sb.append(", ");

                sb.append(name);
                sb.append(" varchar(255)");
            }

            sb.append(", name varchar(255)");
            sb.append(", value varchar(255)");

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

        Map attributes = entry.getAttributes();
        Set set = new HashSet();

        for (Iterator j = attributes.values().iterator(); j.hasNext();) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            if (!attribute.isRdn()) continue;

            String name = attribute.getName();

            if (set.contains(name)) continue;
            set.add(name);

            if (sb.length() > 0) sb.append(", ");
            sb.append(name);
        }

        return sb.toString();
    }

    public String getAttributeNames() {
        StringBuffer sb = new StringBuffer();
        sb.append("name, value");

        return sb.toString();
    }

    public Row getRow(ResultSet rs) throws Exception {
        Row values = new Row();

        int c = 1;

        String name = rs.getString(c++);
        Object value = rs.getObject(c++);

        values.set(name, value);

        return values;
    }

    public Collection search(Row pk) throws Exception {
        List pks = new ArrayList();
        pks.add(pk);

        return search(pks);
    }

    public String getFilter(Collection pks) throws Exception {

        if (pks == null || pks.isEmpty()) return null;

        StringBuffer sb = new StringBuffer();

        for (Iterator i = pks.iterator(); i.hasNext();) {
            Row pk = (Row) i.next();

            StringBuffer sb2 = new StringBuffer();
            for (Iterator j = pk.getNames().iterator(); j.hasNext();) {
                String name = (String) j.next();
                Object value = pk.get(name);

                if (sb2.length() > 0) sb2.append(" and ");

                sb2.append("lower(");
                sb2.append(name);
                sb2.append(")=lower('");
                sb2.append(value);
                sb2.append("')");
            }

            if (sb.length() > 0) sb.append(" or ");

            sb.append(sb2);
        }

        return sb.toString();
    }

    public Collection search(Collection pks) throws Exception {

        if (pks == null || pks.isEmpty()) return new ArrayList();

        String attributeNames = getAttributeNames();

        Filter filter = cache.getCacheContext().getFilterTool().createFilter(pks);
        String sqlFilter = cache.getCacheFilterTool().toSQLFilter(entry, filter, false);
        //String sqlFilter = getFilter(pks);

        String sql = "select " + attributeNames + " from " + tableName
                + " where " + sqlFilter;

        sql += " order by "+getPkAttributeNames();

        List results = new ArrayList();

        log.debug("Executing " + sql);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ds.getConnection();
            ps = con.prepareStatement(sql);

            int counter = 0;
            for (Iterator i=pks.iterator(); i.hasNext(); ) {
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
                Row row = getRow(rs);
                results.add(row);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }

        return results;
    }

    public void insert(Row pk, String name, Object value) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();

            Set set = new HashSet();
            Collection rdnAttributes = entry.getRdnAttributes();

            for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
                AttributeDefinition attributeDefinition = (AttributeDefinition) i.next();
                String attrName = attributeDefinition.getName();

                // TODO need to handle multiple attribute definitions
                if (set.contains(attrName)) continue;
                set.add(attrName);

                if (sb.length() > 0) {
                    sb.append(", ");
                    sb2.append(", ");
                }

                sb.append(attrName);
                sb2.append("?");
            }

            sb.append(", name, value");
            sb2.append(", ?, ?");

            String sql = "insert into " + tableName + " (" + sb + ") values (" + sb2 + ")";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            set = new HashSet();
            int index = 0;

            for (Iterator i = rdnAttributes.iterator(); i.hasNext(); ) {
                AttributeDefinition attributeDefinition = (AttributeDefinition) i.next();
                String attrName = attributeDefinition.getName();

                // TODO need to handle multiple attribute definitions
                if (set.contains(attrName)) continue;
                set.add(attrName);

                Object attrValue = pk.get(attrName);
                String string;
                if (attrValue == null) {
                    string = null;
                } else if (attrValue instanceof byte[]) {
                    string = new String((byte[]) attrValue);
                } else if (attrValue instanceof String) {
                    string = (String) attrValue;
                } else {
                    string = attrValue.toString();
                }

                ps.setString(++index, string);
                log.debug("- " + index + " = " + string);
            }

            ps.setString(++index, name);
            log.debug("- " + index + " = " + name);
            ps.setObject(++index, value);
            log.debug("- " + index + " = " + value);

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

    public void delete(String filter, Date date) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            String sql = "delete from " + tableName;

            if (filter != null) {
                sql += " where "+filter;
            }

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
}
